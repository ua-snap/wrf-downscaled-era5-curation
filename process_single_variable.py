#!/usr/bin/env python3
"""Process a single ERA5 variable for a single year.

This script processes one ERA5 variable for one year, using Dask for parallelization
within a single compute node. It reads the hourly ERA5 data, applies the appropriate
aggregation function, regrids to EPSG:3338, and writes the output to a NetCDF file.

Example usage:
    python process_single_variable.py --year 1980 --variable t2_mean 
    python process_single_variable.py --year 1990 --variable rainnc_sum --input_dir /path/to/data --output_dir /path/to/output
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Union
import time
import gc

import numpy as np
import xarray as xr
import rioxarray
from pyproj import CRS, Transformer, Proj


from era5_variables import era5_datavar_lut
from config import config
from utils.memory import start as start_mem_monitor, stop as stop_mem_monitor
from utils.dask_utils import (
    get_dask_client, 
    configure_dask_memory
)
from utils.logging import get_logger, setup_variable_logging

# Batch size for file processing to reduce metadata contention
BATCH_SIZE = 30

# Get a named logger for this module
logger = get_logger(__name__)

def parse_args() -> argparse.Namespace:
    """Parse command line arguments.
    
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(description=__doc__)
    
    # Required arguments
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Year to process"
    )
    parser.add_argument(
        "--variable",
        type=str,
        required=True,
        help="Variable to process"
    )
    
    # Optional arguments with defaults
    parser.add_argument(
        "--input_dir",
        type=Path,
        default=config.INPUT_DIR,
        help=f"Directory containing ERA5 data (default: {config.INPUT_DIR})"
    )
    parser.add_argument(
        "--output_dir",
        type=Path,
        default=config.OUTPUT_DIR,
        help=f"Directory for output files (default: {config.OUTPUT_DIR})"
    )
    parser.add_argument(
        "--geo_file",
        type=Path,
        default=config.GEO_EM_FILE,
        help=f"Path to WRF geo_em file for projection information (default: {config.GEO_EM_FILE})"
    )
    parser.add_argument(
        "--fn_str",
        type=str,
        default=config.INPUT_FILE_PATTERN,
        help=f"File pattern for input files (default: {config.INPUT_FILE_PATTERN})"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing output files"
    )
    
    # Dask configuration
    parser.add_argument(
        "--cores",
        type=int,
        default=None,
        help="Number of cores to use (default: auto-detect)"
    )
    parser.add_argument(
        "--memory_limit",
        type=str,
        default="16GB",
        help="Memory limit for Dask workers (default: 16GB)"
    )
    args = parser.parse_args()
    
    # Validate the variable
    if args.variable not in era5_datavar_lut:
        logger.error(f"Variable '{args.variable}' not found in ERA5 variable lookup table")
        available_vars = list(era5_datavar_lut.keys())
        logger.error(f"Available variables: {', '.join(available_vars[:10])}...")
        sys.exit(1)
    
    return args


def get_year_filepaths(era5_dir: Path, year: int, fn_str: str) -> List[Path]:
    """Get all filepaths for a single year of ERA5 data.

    Args:
        era5_dir: Directory containing ERA5 data
        year: Year to process
        fn_str: Filename pattern for input files
    
    Returns:
        List of filepaths for the year
    """
    year_dir = era5_dir.joinpath(str(year))
    if not year_dir.exists():
        raise FileNotFoundError(f"Year directory not found: {year_dir}")
    
    # Use glob to find all files matching the pattern
    # Format the date part of the pattern with the year and a wildcard for month/day
    date_pattern = f"{year}-*"
    file_pattern = fn_str.format(date=date_pattern)
    
    fps = sorted(year_dir.glob(file_pattern))
    
    if not fps:
        raise FileNotFoundError(f"No files found for year {year} with pattern {file_pattern}")
    
    logger.info(f"Found {len(fps)} files for year {year}")
    return fps


def check_output_exists(output_dir: Path, variable: str, year: int) -> Tuple[bool, Path]:
    """Check if output file already exists.

    Args:
        output_dir: Output directory
        variable: Variable name
        year: Year to process
    
    Returns:
        Tuple of (exists, output_path)
    """
    # Create variable subdirectory if it doesn't exist
    var_dir = output_dir.joinpath(variable)
    var_dir.mkdir(exist_ok=True, parents=True)
    
    # Output filename: variable_year_daily_era5_4km_3338.nc
    output_file = var_dir.joinpath(f"{variable}_{year}_daily_era5_4km_3338.nc")
    
    exists = output_file.exists()
    if exists:
        logger.info(f"Output file already exists: {output_file}")
    
    return exists, output_file


def is_3d_variable(variable: str) -> bool:
    """Determine if a variable is 3D (has vertical levels).
    
    Args:
        variable: Variable name
        
    Returns:
        True if the variable is 3D, False otherwise
    """
    # Get source variable ID
    if variable not in era5_datavar_lut:
        raise ValueError(f"Variable {variable} not found in ERA5 variable lookup table")
    
    source_var = era5_datavar_lut[variable]["var_id"]
    
    # Variables with 'interp_level' or 'soil_layers_stag' dimension are 3D
    # This is based on the NetCDF structure shown in input_data_specs
    return source_var in [
        "dbz", "twb", "rh", "temp", "height", "CLDFRA", "QVAPOR", "u", "v", "w",
        "SMOIS", "SH2O", "TSLB"
    ]


def get_variable_chunks(variable: str) -> Dict[str, Union[str, int]]:
    """Get optimal chunk sizes for a variable.
    
    Configures chunking with no time chunking (-1 for Time dimension) to optimize
    performance, since memory usage is typically low. Spatial dimensions are kept
    as full chunks to avoid splitting.
    
    Args:
        variable: Variable name
    
    Returns:
        Dictionary of chunk sizes
    """
    # For 3D variables, use smaller chunks in time and level dimensions
    if is_3d_variable(variable):
        return {
            "Time": -1,          # Use entire time dimension (no chunking)
            "interp_level": -1,  # Include all levels in each chunk
            "south_north": -1,   # Use full chunks for south_north to avoid splitting
            "west_east": -1      # Use full chunks for west_east to avoid splitting
        }
    else:
        # For 2D variables, we can use larger chunks in time
        return {
            "Time": -1,          # Use entire time dimension (no chunking)
            "south_north": -1,   # Use full chunks for south_north to avoid splitting
            "west_east": -1      # Use full chunks for west_east to avoid splitting
        }


def get_grid_info(tmp_file, geo_file):
    ds = xr.open_dataset(tmp_file)
    geo_ds = xr.open_dataset(geo_file)

    # The proj4 string for the WRF projection is:
    # +proj=stere +units=m +a=6370000.0 +b=6370000.0 +lat_0=90.0 +lon_0=-152 +lat_ts=64 +nadgrids=@null
    # this was determined separately using the WRF-Python package
    # which has spotty availability / compatability
    #
    # here is the code for how that was done:
    # wrf_proj = PolarStereographic(
    #     **{"TRUELAT1": geo_ds.attrs["TRUELAT1"], "STAND_LON": geo_ds.attrs["STAND_LON"]}
    # ).proj4()
    wrf_proj = "+proj=stere +units=m +a=6370000.0 +b=6370000.0 +lat_0=90.0 +lon_0=-152 +lat_ts=64 +nadgrids=@null"

    # WGS84 projection
    wgs_proj = Proj(proj="latlong", datum="WGS84")
    wgs_to_wrf_transformer = Transformer.from_proj(wgs_proj, wrf_proj)

    # this is where we plug in the center longitude of the domain to get the center x, y in projected space
    e, n = wgs_to_wrf_transformer.transform(
        geo_ds.attrs["CEN_LON"], geo_ds.attrs["TRUELAT1"]
    )
    # now compute the rest of the grid based on x/y dimension lengths and grid spacing
    dx = dy = 4000
    nx = ds.XLONG.shape[1]
    ny = ds.XLONG.shape[0]
    x0 = -(nx - 1) / 2.0 * dx + e
    y0 = -(ny - 1) / 2.0 * dy + n
    # 2d grid coordinate values
    x = np.arange(nx) * dx + x0
    y = np.arange(ny) * dy + y0

    wrf_crs = CRS.from_proj4(wrf_proj)

    return {"x": x, "y": y, "wrf_crs": wrf_crs}


def read_data(filepaths: List[Path], variable: str, chunks: Dict[str, Union[str, int]]) -> xr.Dataset:
    """Read ERA5 data for processing.
    
    Args:
        filepaths: List of input file paths
        variable: Variable name to process
        chunks: Chunk sizes for Dask
    
    Returns:
        xarray Dataset with the data
    """
    # Get the source variable ID
    source_var = era5_datavar_lut[variable]["var_id"]
    
    # Identify variables to drop (all except the ones we need)
    # This significantly reduces memory usage when opening files
    keep_vars = [source_var, "Time", "south_north", "west_east", "XLONG", "XLAT"]
    
    # Open a sample file to get the list of all variables
    with xr.open_dataset(filepaths[0]) as sample_ds:
        all_vars = list(sample_ds.variables)
        
        # Get the original chunks from the sample file to ensure we don't split chunks
        # This should prevent the warnings about separating stored chunks
        original_chunks = {dim: sample_ds[dim].shape[0] for dim in sample_ds.dims 
                          if dim in ['south_north', 'west_east']}
        
        # Determine optimal chunks based on original file structure
        if chunks.get('south_north') == -1 and 'south_north' in original_chunks:
            chunks['south_north'] = original_chunks['south_north']
        if chunks.get('west_east') == -1 and 'west_east' in original_chunks:
            chunks['west_east'] = original_chunks['west_east']
    
    drop_vars = [v for v in all_vars if v not in keep_vars]
    
    logger.info(f"Opening files with chunks: {chunks}")
    
    # Open the dataset with Dask
    ds = xr.open_mfdataset(
        filepaths,
        drop_variables=drop_vars,
        chunks=chunks,
        parallel=True,
        engine="h5netcdf",
        combine="by_coords"  # Ensure proper combination by coordinates
    )
    
    logger.info(f"Opened {len(filepaths)} files with Dask")
    return ds


def process_variable(ds: xr.Dataset, variable: str) -> xr.Dataset:
    """Process a variable by applying the aggregation function.
    
    Args:
        ds: Input xarray Dataset
        variable: Variable name to process
    
    Returns:
        xarray Dataset with processed data
    """
    # Get variable info and aggregation function
    var_info = era5_datavar_lut[variable]
    source_var = var_info["var_id"]
    agg_func = var_info["agg_func"]
    
    # Apply the aggregation function to resample from hourly to daily
    logger.info(f"Resampling variable {variable} ({source_var}) to daily frequency")
    
    # Extract the variable data array and resample
    da = ds[source_var]
    # Resample to daily frequency and apply the aggregation function
    resampled = da.resample(Time="1D").map(agg_func).rename(variable)
    # Create a new dataset with the resampled data
    result_ds = resampled.to_dataset()
    
    # Copy attributes from the original variable
    if hasattr(da, "attrs"):
        resampled.attrs.update(da.attrs)
    # Assign CF standard attributes using info from the lookup table
    resampled.attrs["long_name"] = var_info.get("description", f"Daily {variable}") 
    resampled.attrs["units"] = var_info.get("units", "unknown")
    resampled.attrs["standard_name"] = var_info.get("standard_name", "unknown")
    # Remove the old description and projection if they exist in the original data
    resampled.attrs.pop("description", None)
    resampled.attrs.pop("projection", None)
    
    # Re-assign the modified DataArray back to the dataset
    result_ds[variable] = resampled
    
    return result_ds


def regrid_to_3338(ds: xr.Dataset, grid_info: Dict[str, Any]) -> xr.Dataset:
    """Reproject data to Alaska Albers (EPSG:3338).
    
    Args:
        ds: Input xarray Dataset
        grid_info: Grid information from get_grid_info
    
    Returns:
        xarray Dataset reprojected to EPSG:3338
    """
    x, y, wrf_crs = [grid_info[k] for k in ["x", "y", "wrf_crs"]]

    ds_proj = (
        ds.rename({"south_north": "y", "west_east": "x"})
        .assign_coords({"y": ("y", y), "x": ("x", x)})
        .drop_vars(["XLONG", "XLAT"])
        .rio.set_spatial_dims("x", "y")
        .rio.write_crs(wrf_crs)
    )

    ds_3338 = ds_proj.rio.reproject("EPSG:3338")
    return ds_3338


def process_files_in_batches(
    filepaths: List[Path], 
    variable: str, 
    chunks: Dict[str, Union[str, int]]
) -> xr.Dataset:
    """Process files in smaller batches to reduce metadata contention.
    
    Args:
        filepaths: List of input file paths
        variable: Variable name to process
        chunks: Chunk sizes for Dask
    
    Returns:
        xarray Dataset with data from all batches
    """
    # Calculate number of batches
    num_files = len(filepaths)
    num_batches = (num_files + BATCH_SIZE - 1) // BATCH_SIZE  # Ceiling division
    
    logger.info(f"Processing {num_files} files in {num_batches} batches of {BATCH_SIZE}")
    
    # Initialize with empty dataset
    combined_ds = None
    
    # Process each batch
    for i in range(num_batches):
        start_idx = i * BATCH_SIZE
        end_idx = min((i + 1) * BATCH_SIZE, num_files)
        batch_files = filepaths[start_idx:end_idx]
        
        logger.info(f"Processing batch {i+1}/{num_batches} with {len(batch_files)} files")
        
        # Read this batch of files
        batch_ds = read_data(batch_files, variable, chunks)
        
        # If this is the first batch, use it as the base dataset
        if combined_ds is None:
            combined_ds = batch_ds
        else:
            # Combine with previous batches along the time dimension
            combined_ds = xr.concat([combined_ds, batch_ds], dim="Time")
            # Clean up batch dataset to free memory
            del batch_ds
            gc.collect()
    
    logger.info(f"Successfully processed all {num_batches} batches")
    return combined_ds


def write_output(ds: xr.Dataset, variable: str, output_file: Path) -> None:
    """Write output to NetCDF file following CF conventions.
    
    Args:
        ds: xarray Dataset to write
        variable: The primary variable name in the dataset
        output_file: Output file path
    """
    # Ensure output directory exists
    output_file.parent.mkdir(exist_ok=True, parents=True)
    
    # CF Compliant Attributes 
    current_time = time.strftime("%Y-%m-%d %H:%M:%S UTC")
    ds.attrs["Conventions"] = "CF-1.8"
    ds.attrs["title"] = f"Daily aggregated {variable} from WRF-downscaled ERA5, 4km Alaska Albers"
    ds.attrs["institution"] = "Alaska Climate Adaptation Science Center, University of Alaska Fairbanks"
    ds.attrs["source"] = "Hourly WRF-downscaled ERA5"
    ds.attrs["history"] = f"{current_time}: Created using {Path(__file__).name}"
    ds.attrs["comment"] = f"Data processed for variable '{variable}' to daily resolution and reprojected to EPSG:3338."
    ds.attrs["references"] = "Placeholder" # Example reference, update as needed

    # Rename Time to time
    ds = ds.rename({"Time": "time"})
    ds.coords["time"].attrs["standard_name"] = "time"
    ds.coords["time"].attrs["axis"] = "T"

    # Set spatial dimensions
    ds = ds.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)

    encoding = {
        var: {"zlib": True, "complevel": 5} 
        for var in ds.data_vars 
    }

    # Write to NetCDF file
    logger.info(f"Writing output to {output_file}")
    ds.to_netcdf(
        output_file,
        engine="h5netcdf",
        encoding=encoding
    )
    logger.info(f"Successfully wrote output to {output_file}")


def process_variable_for_year(
    variable: str,
    year: int,
    filepaths: List[Path],
    geo_file: Path,
    output_dir: Path,
    cores: Optional[int] = None,
    memory_limit: str = "16GB",
    overwrite: bool = False
) -> Optional[Path]:
    """Process a single variable for a single year.
    
    This function processes one ERA5 variable for one year, using Dask for
    parallelization within a single compute node. It reads the hourly ERA5 data,
    applies the appropriate aggregation function, regrids to EPSG:3338, and writes
    the output to a NetCDF file.
    
    Files are processed in small batches (defined by BATCH_SIZE) to reduce metadata 
    contention on the filesystem.
    
    Args:
        variable: Variable to process
        year: Year to process
        filepaths: List of input file paths
        geo_file: Path to WRF geo_em file for projection information
        output_dir: Directory for output files
        cores: Number of cores to use (default: auto-detect)
        memory_limit: Memory limit for Dask workers
        overwrite: Whether to overwrite existing output files
    
    Returns:
        Path to the output file if successful, None otherwise
    """
    # Check if output already exists
    exists, output_file = check_output_exists(output_dir, variable, year)
    if exists and not overwrite:
        logger.info(f"Output file {output_file} already exists, skipping (overwrite=False)")
        return None
    
    # Initialize client variables outside try block
    io_client = io_cluster = compute_client = compute_cluster = None
    
    try:
        # Configure Dask memory settings
        configure_dask_memory()
        
        # Get optimal chunk sizes for this variable
        chunks = get_variable_chunks(variable)
        
        # ===== I/O PHASE =====
        logger.info("Starting I/O phase with io_bound configuration")
        io_client, io_cluster = get_dask_client(cores, memory_limit, "io_bound")
        
        # Get grid information
        logger.info(f"Retrieving grid information from {geo_file}")
        grid_info = get_grid_info(filepaths[0], geo_file)
        
        # Read input data in batches to reduce metadata contention
        logger.info(f"Reading data for {variable} from {len(filepaths)} files in batches of {BATCH_SIZE}")
        ds = process_files_in_batches(filepaths, variable, chunks)
        
        # Process the variable - this is mostly I/O and simple aggregation
        logger.info(f"Processing variable {variable}")
        processed_ds = process_variable(ds, variable)
        
        # Clean up intermediate data to free memory
        del ds
        io_client.run(lambda: gc.collect())  # Trigger garbage collection on workers
        
        # Close I/O client
        logger.info("Closing I/O phase Dask client")
        io_client.close()
        io_cluster.close()
        
        # ===== COMPUTATION PHASE =====
        logger.info("Starting computation phase with balanced configuration")
        compute_client, compute_cluster = get_dask_client(cores, memory_limit, "balanced")
        
        # Regrid to EPSG:3338 - computation intensive
        logger.info(f"Reprojecting data to EPSG:3338")
        reprojected_ds = regrid_to_3338(processed_ds, grid_info)
        
        # Clean up intermediate data to free memory
        del processed_ds
        compute_client.run(lambda: gc.collect())  # Trigger garbage collection on workers
        
        write_output(reprojected_ds, variable, output_file)
        
        # Clean up final data
        del reprojected_ds
        compute_client.run(lambda: gc.collect())  # Trigger garbage collection on workers
        
        return output_file
    
    except RecursionError as e:
        logger.error(f"RecursionError processing {variable} for year {year}: {e}")
        logger.error("Try increasing dask.config.set({'distributed.worker.memory.sizeof.sizeof-recurse-limit': 100})")
        import traceback
        logger.error(traceback.format_exc())
        return None
    except MemoryError as e:
        logger.error(f"MemoryError processing {variable} for year {year}: {e}")
        logger.error("Try increasing memory_limit in the SLURM script and reducing chunk sizes")
        import traceback
        logger.error(traceback.format_exc())
        return None
    except Exception as e:
        logger.error(f"Error processing {variable} for year {year}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None
    
    finally:
        # Close the clients and clusters if they were created
        if io_client is not None:
            try:
                io_client.close()
            except:
                pass
        if io_cluster is not None:
            try:
                io_cluster.close()
            except:
                pass
        
        if compute_client is not None:
            try:
                compute_client.close()
            except:
                pass
        if compute_cluster is not None:
            try:
                compute_cluster.close()
            except:
                pass
            
        logger.info(f"Closed all Dask clients and clusters")


def main() -> None:
    """Main processing function."""
    args = parse_args()
    
    # Set up logging using the centralized variable logging function
    setup_variable_logging(
        variable=args.variable,
        year=args.year,
        base_dir=Path.cwd(),
    )
    
    start_mem_monitor()
    try:
        # Set up paths
        input_dir = args.input_dir
        output_dir = args.output_dir
        year = args.year
        variable = args.variable
        
        # Process the variable
        result = process_variable_for_year(
            variable=variable,
            year=year,
            filepaths=get_year_filepaths(input_dir, year, args.fn_str),
            geo_file=args.geo_file,
            output_dir=output_dir,
            cores=args.cores,
            memory_limit=args.memory_limit,
            overwrite=args.overwrite
        )
        
        if result:
            logger.info(f"Successfully processed {variable} for year {year}")
            sys.exit(0)
        else:
            logger.error(f"Failed to process {variable} for year {year}")
            sys.exit(1)
    finally:
        stop_mem_monitor()


if __name__ == "__main__":
    main() 