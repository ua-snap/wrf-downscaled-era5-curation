#!/usr/bin/env python3
"""Process a single ERA5 variable for a single year.

This script processes one ERA5 variable for one year, using Dask for parallelization
within a single compute node. It reads the hourly ERA5 data, applies the appropriate
aggregation function, regrids to EPSG:3338, and writes the output to a NetCDF file.

Configuration is handled through environment variables:
    ERA5_INPUT_DIR: Input directory containing ERA5 data
    ERA5_OUTPUT_DIR: Output directory for processed files
    ERA5_GEO_FILE: Path to WRF geo_em file for projection information
    ERA5_DASK_CORES: Number of cores to use (default: auto-detect)
    ERA5_DASK_MEMORY_LIMIT: Memory limit for Dask workers (default: 85GB)
    ERA5_DASK_TASK_TYPE: Task type for Dask workers (default: balanced)

Example usage:
    # Basic usage with default configuration
    python process_single_variable.py --year 1980 --variable t2_mean 

    # Force reprocessing of existing files
    python process_single_variable.py --year 1980 --variable t2_mean --overwrite
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
from config import data_config, config
from utils.dask_utils import (
    get_dask_client, 
    configure_dask_memory
)
from utils.logging import get_logger, setup_variable_logging

logger = get_logger(__name__)

def parse_args() -> argparse.Namespace:
    """Parse command line arguments.
    
    Returns:
        Parsed arguments
        
    Raises:
        ValueError: If an invalid variable is provided
    """
    parser = argparse.ArgumentParser(
        description="Process a single ERA5 variable for a single year"
    )
    parser.add_argument(
        "--variable", 
        required=True, 
        help="Variable to process"
    )
    parser.add_argument(
        "--year", 
        type=int, 
        required=True, 
        help="Year to process"
    )
    parser.add_argument(
        "--overwrite", 
        action="store_true", 
        help="Overwrite existing output files"
    )
    
    args = parser.parse_args()
    
    # Validate the variable
    if args.variable not in era5_datavar_lut:
        available_vars = list(era5_datavar_lut.keys())
        raise ValueError(
            f"Variable '{args.variable}' not found in ERA5 variable lookup table. "
            f"Available variables: {', '.join(available_vars[:10])}..."
        )
    
    return args

def get_year_filepaths(year: int) -> List[Path]:
    """Get all filepaths for a single year of ERA5 data.
    
    Args:
        year: Year to process
    
    Returns:
        List of filepaths for the year
    """
    year_dir = data_config.get_year_dir(year)
    if not year_dir.exists():
        raise FileNotFoundError(f"Year directory not found: {year_dir}")
    
    # Use glob to find all files matching the pattern
    # Format the date part of the pattern with the year and a wildcard for month/day
    date_pattern = f"{year}-*"
    file_pattern = data_config.file_pattern.format(date=date_pattern)
    
    fps = sorted(year_dir.glob(file_pattern))
    
    if not fps:
        raise FileNotFoundError(f"No files found for year {year} with pattern {file_pattern}")
    
    logger.info(f"Found {len(fps)} files for year {year}")
    return fps

def check_output_exists(variable: str, year: int) -> Tuple[bool, Path]:
    """Check if output file already exists.
    
    Args:
        variable: Variable name
        year: Year to process
    
    Returns:
        Tuple of (exists, output_path)
    """
    output_file = data_config.get_output_file(variable, year)
    
    # Create variable subdirectory if it doesn't exist
    output_file.parent.mkdir(exist_ok=True, parents=True)
    
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
    num_batches = (num_files + config.BATCH_SIZE - 1) // config.BATCH_SIZE 
    
    logger.info(f"Processing {num_files} files in {num_batches} batches of {config.BATCH_SIZE}")
    
    # Initialize with empty dataset
    combined_ds = None
    
    # Process each batch
    for i in range(num_batches):
        start_idx = i * config.BATCH_SIZE
        end_idx = min((i + 1) * config.BATCH_SIZE, num_files)
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
    overwrite: bool = False
) -> Path:
    """Process a single variable for a single year.
    
    This function processes one ERA5 variable for one year, using Dask for
    parallelization within a single compute node. It reads the hourly ERA5 data,
    applies the appropriate aggregation function, regrids to EPSG:3338, and writes
    the output to a NetCDF file.
    
    Files are processed in small batches (defined by BATCH_SIZE) to reduce metadata 
    contention on the filesystem.
    
    Dask configuration is handled through environment variables via the config system.
    
    Args:
        variable: Variable to process
        year: Year to process
        overwrite: Whether to overwrite existing output files
    
    Returns:
        Path to the output file if successful
        
    Raises:
        FileNotFoundError: If required input files are not found
        RuntimeError: If processing fails due to computation or I/O errors
        ValueError: If invalid parameters are provided
    """
    # Check if output already exists
    exists, output_file = check_output_exists(variable, year)
    if exists and not overwrite:
        logger.info(f"Output file exists, skipping (overwrite=False)")
        raise RuntimeError(f"Output file {output_file} already exists and overwrite=False")
    
    # Initialize client variables outside try block
    io_client = io_cluster = compute_client = compute_cluster = None
    
    try:
        # Configure Dask memory settings
        configure_dask_memory()
        
        # Get optimal chunk sizes for this variable
        chunks = get_variable_chunks(variable)
        
        # ===== I/O PHASE =====
        logger.info("Starting I/O phase with io_bound configuration")
        io_client, io_cluster = get_dask_client(config.dask.cores, config.dask.memory_limit, "io_bound")
        
        # Get grid information
        logger.info(f"Retrieving grid information from {data_config.geo_file}")
        grid_info = get_grid_info(get_year_filepaths(year)[0], data_config.geo_file)
        
        # Read input data in batches to reduce metadata contention
        filepaths = get_year_filepaths(year)
        logger.info(f"Reading data for {variable} from {len(filepaths)} files in batches of {config.BATCH_SIZE}")
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
        compute_client, compute_cluster = get_dask_client(config.dask.cores, config.dask.memory_limit, "balanced")
        
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
    
    except Exception as e:
        logger.error(f"Error processing {variable} for year {year}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise RuntimeError(f"Processing failed for {variable} year {year}: {e}") from e
    
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
        console_only=True
    )
    
    try:
        # Process the variable
        result = process_variable_for_year(
            variable=args.variable,
            year=args.year,
            overwrite=args.overwrite
        )
        
        logger.info(f"Successfully processed {args.variable} for year {args.year}")
        logger.info(f"Output written to: {result}")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Failed to process {args.variable} for year {args.year}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 