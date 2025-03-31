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
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Union
import time
import gc

import numpy as np
import xarray as xr
import rioxarray
from pyproj import CRS, Transformer, Proj
from dask.distributed import LocalCluster, Client, performance_report
import dask

# Add recursion limit configuration for Dask to prevent RecursionError
dask.config.set({"distributed.worker.memory.sizeof.sizeof-recurse-limit": 50})

from era5_variables import era5_datavar_lut
from config import config
from utils.memory import start as start_mem_monitor, stop as stop_mem_monitor


def setup_logging(verbose: bool = False) -> None:
    """Set up logging configuration.
    
    Logs are written to both console and a file in the user's home directory
    (~/era5_processing.log). If the home directory is not accessible, falls back
    to console-only logging.
    
    Args:
        verbose: Whether to use verbose logging
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    
    try:
        # Create log file in user's home directory
        log_file = Path.home() / "era5_processing.log"
        handlers = [
            logging.StreamHandler(),
            logging.FileHandler(log_file)
        ]
    except Exception as e:
        logging.warning(f"Could not access home directory for log file: {e}")
        logging.warning("Falling back to console-only logging")
        handlers = [logging.StreamHandler()]
    
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers
    )


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
    parser.add_argument(
        "--optimization_mode",
        type=str,
        choices=["balanced", "io_optimized", "compute_optimized", "fully_optimized"],
        default="io_optimized",
        help="Optimization mode for Dask worker configuration (default: io_optimized)"
    )
    parser.add_argument(
        "--generate_report",
        action="store_true",
        help="Generate a Dask performance report"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )

    # Memory monitoring options
    parser.add_argument(
        "--recurse_limit",
        type=int,
        default=50,
        help="Recursion limit for Dask sizeof function (default: 50)"
    )
    
    args = parser.parse_args()
    
    # Validate the variable
    if args.variable not in era5_datavar_lut:
        logging.error(f"Variable '{args.variable}' not found in ERA5 variable lookup table")
        available_vars = list(era5_datavar_lut.keys())
        logging.error(f"Available variables: {', '.join(available_vars[:10])}...")
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
    
    logging.info(f"Found {len(fps)} files for year {year}")
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
    
    # Output filename: variable_year_era5_4km_3338.nc
    output_file = var_dir.joinpath(f"{variable}_{year}_era5_4km_3338.nc")
    
    exists = output_file.exists()
    if exists:
        logging.info(f"Output file already exists: {output_file}")
    
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


def calculate_worker_config(cores: int, task_type: str = "balanced") -> Tuple[int, int]:
    """Calculate optimal worker count and threads per worker based on core count and task type.
    
    Provides different configurations based on the type of processing task:
    - "io_bound": More workers with fewer threads, optimal for file operations
    - "compute_bound": Fewer workers with more threads, better for CPU-intensive tasks
    - "balanced": Balanced approach using sqrt formula (default)
    
    Args:
        cores: Number of available CPU cores
        task_type: Type of task being performed ("io_bound", "compute_bound", or "balanced")
        
    Returns:
        Tuple of (worker_count, threads_per_worker)
    """
    import math
    
    if task_type == "io_bound":
        # More workers with fewer threads for I/O operations
        worker_count = min(cores, 12)  # Up to 12 workers, but no more than cores
        threads_per_worker = max(1, cores // worker_count)
    elif task_type == "compute_bound":
        # Fewer workers with more threads for computation
        worker_count = max(4, cores // 4)  # At least 4 workers
        threads_per_worker = max(2, cores // worker_count)
    else:  # balanced
        # Square root approach for balanced workloads (default behavior)
        worker_count = max(2, min(8, round(math.sqrt(cores))))
        threads_per_worker = max(1, cores // worker_count)
    
    # Ensure total threads don't exceed core count
    total_threads = worker_count * threads_per_worker
    if total_threads > cores:
        # Adjust threads per worker down if needed
        threads_per_worker = max(1, cores // worker_count)
    
    return worker_count, threads_per_worker


def calculate_worker_memory(total_memory_gb: float, n_workers: int, task_type: str = "balanced") -> str:
    """Calculate appropriate memory per worker based on available memory and task type.
    
    Args:
        total_memory_gb: Total memory available in GB
        n_workers: Number of workers being created
        task_type: Type of task being performed
        
    Returns:
        Memory limit per worker as a string (e.g., "4GB")
    """
    # Minimum reasonable memory per worker
    min_memory_per_worker_gb = 2  # 2GB minimum
    
    # Calculate memory based on task type
    if task_type == "io_bound":
        # I/O tasks typically need less memory
        worker_fraction = 0.7  # Use 70% of available memory
        memory_factor = 0.8    # Each worker gets less memory
    elif task_type == "compute_bound":
        # Compute tasks may need more memory
        worker_fraction = 0.8  # Use 80% of available memory
        memory_factor = 1.2    # Each worker gets more memory
    else:  # balanced
        worker_fraction = 0.75  # Use 75% of available memory
        memory_factor = 1.0    # Standard memory allocation
    
    # Calculate memory per worker with task-specific adjustments
    safe_total_gb = total_memory_gb * worker_fraction
    base_worker_memory_gb = safe_total_gb / n_workers
    adjusted_worker_memory_gb = base_worker_memory_gb * memory_factor
    
    # Ensure we meet minimum memory requirements
    worker_memory_gb = max(min_memory_per_worker_gb, adjusted_worker_memory_gb)
    
    # Return formatted memory string
    return f"{int(worker_memory_gb)}GB"


def get_dask_client(cores: Optional[int] = None, 
                   memory_limit: str = "16GB", 
                   task_type: str = "balanced") -> Tuple[Client, LocalCluster]:
    """Set up a Dask LocalCluster and Client optimized for specific task types.
    
    Automatically determines optimal worker count, threads per worker, and
    memory allocation based on available resources and the type of task:
    - "io_bound": Optimized for I/O operations (more workers, less memory per worker)
    - "compute_bound": Optimized for computation (fewer workers, more threads and memory)
    - "balanced": Balanced configuration for mixed workloads
    
    Args:
        cores: Number of cores to use (None for auto-detect)
        memory_limit: Total memory limit across all workers
        task_type: Type of task being performed
    
    Returns:
        Tuple of (Client, LocalCluster)
    """
    # For SLURM jobs, use the allocated resources
    if "SLURM_JOB_ID" in os.environ:
        # If SLURM_CPUS_PER_TASK is set, use that for the number of cores
        if "SLURM_CPUS_PER_TASK" in os.environ:
            slurm_cores = int(os.environ["SLURM_CPUS_PER_TASK"])
            cores = slurm_cores if cores is None else min(cores, slurm_cores)
            logging.info(f"Using {cores} cores from SLURM allocation")
        
        # Check for memory allocation in SLURM
        if "SLURM_MEM_PER_NODE" in os.environ:
            slurm_mem_mb = int(os.environ["SLURM_MEM_PER_NODE"])
            slurm_mem_gb = slurm_mem_mb / 1024
            logging.info(f"SLURM memory allocation: {slurm_mem_gb:.1f}GB")
    
    # If cores is still None, use CPU count
    if cores is None:
        cores = os.cpu_count()
    
    # Calculate worker configuration based on task type
    n_workers, threads_per_worker = calculate_worker_config(cores, task_type)
    
    # Calculate total memory available (from memory_limit string)
    if isinstance(memory_limit, str):
        # Parse memory string like "16GB"
        import re
        memory_value = float(re.match(r'(\d+(\.\d+)?)', memory_limit).group(1))
        if "MB" in memory_limit:
            memory_gb = memory_value / 1024
        elif "GB" in memory_limit:
            memory_gb = memory_value
        else:
            # Default to bytes, convert to GB
            memory_gb = float(memory_limit) / (1024 * 1024 * 1024)
    else:
        memory_gb = float(memory_limit) / (1024 * 1024 * 1024)
    
    # Calculate memory per worker
    memory_per_worker = calculate_worker_memory(memory_gb, n_workers, task_type)
    
    # Set up a local cluster with appropriate resources
    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        memory_limit=memory_per_worker
    )
    
    logging.info(f"Created Dask LocalCluster [{task_type} mode] with {n_workers} workers, "
                 f"{threads_per_worker} threads per worker, and {memory_per_worker} memory per worker")
    
    # Create a client
    client = Client(cluster)
    logging.info(f"Dask dashboard available at: {client.dashboard_link}")
    
    return client, cluster


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
    
    logging.info(f"Opening files with chunks: {chunks}")
    
    # Open the dataset with Dask
    ds = xr.open_mfdataset(
        filepaths,
        drop_variables=drop_vars,
        chunks=chunks,
        parallel=True,
        engine="h5netcdf",
        combine="by_coords"  # Ensure proper combination by coordinates
    )
    
    logging.info(f"Opened {len(filepaths)} files with Dask")
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
    logging.info(f"Resampling variable {variable} ({source_var}) to daily frequency")
    
    # Extract the variable data array and resample
    da = ds[source_var]
    
    # Resample to daily frequency and apply the aggregation function
    resampled = da.resample(Time="1D").map(agg_func).rename(variable)
    
    # Create a new dataset with the resampled data
    result_ds = resampled.to_dataset()
    
    # Copy attributes from the original variable
    if hasattr(da, "attrs"):
        resampled.attrs.update(da.attrs)
    
    # Add a description attribute
    resampled.attrs["description"] = var_info.get("description", f"Daily {variable}")
    
    # Rename Time to time for consistency
    if "Time" in result_ds.dims:
        result_ds = result_ds.rename({"Time": "time"})
    
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


def write_output(ds: xr.Dataset, output_file: Path) -> None:
    """Write output to NetCDF file.
    
    Args:
        ds: xarray Dataset to write
        output_file: Output file path
    """
    # Ensure output directory exists
    output_file.parent.mkdir(exist_ok=True, parents=True)
    
    # Add global attributes
    ds.attrs["creation_date"] = time.strftime("%Y-%m-%d %H:%M:%S")
    ds.attrs["source"] = "WRF ERA5 Downscaled Data, Hourly Resolution"
    ds.attrs["projection"] = "EPSG:3338 (Alaska Albers)"
    ds.attrs["institution"] = "Alaska Climate Adaptation Science Center"
    ds.attrs["credit"] = "Chris Waigl"
    
    # Write to NetCDF file
    logging.info(f"Writing output to {output_file}")
    ds.to_netcdf(
        output_file,
        engine="h5netcdf",
        encoding={
            var: {"zlib": True, "complevel": 5} for var in ds.data_vars
        }
    )
    logging.info(f"Successfully wrote output to {output_file}")


def process_variable_for_year(
    variable: str,
    year: int,
    filepaths: List[Path],
    geo_file: Path,
    output_dir: Path,
    generate_report: bool = False,
    cores: Optional[int] = None,
    memory_limit: str = "16GB",
    optimization_mode: str = "io_optimized",
    overwrite: bool = False
) -> None:
    """Process a single variable for a single year.
    
    This function processes one ERA5 variable for one year, using Dask for
    parallelization within a single compute node. It reads the hourly ERA5 data,
    applies the appropriate aggregation function, regrids to EPSG:3338, and writes
    the output to a NetCDF file. If generate_report is True, a Dask performance
    report is written to the user's home directory (~/{variable}_{year}_performance.html).
    If the home directory is not accessible, the report is written to the current directory.
    
    Args:
        variable: Variable to process
        year: Year to process
        filepaths: List of input file paths
        geo_file: Path to WRF geo_em file for projection information
        output_dir: Directory for output files
        generate_report: Whether to generate a Dask performance report
        cores: Number of cores to use (default: auto-detect)
        memory_limit: Memory limit for Dask workers
        optimization_mode: Optimization mode ("balanced", "io_optimized", or "compute_optimized")
        overwrite: Whether to overwrite existing output files
    """
    # Map optimization mode to task types for different processing phases
    task_types = {
        "balanced": {"io": "balanced", "compute": "balanced"},
        "io_optimized": {"io": "io_bound", "compute": "balanced"},
        "compute_optimized": {"io": "balanced", "compute": "compute_bound"},
        "fully_optimized": {"io": "io_bound", "compute": "compute_bound"}
    }
    
    # Use balanced mode as default if unknown optimization mode provided
    if optimization_mode not in task_types:
        logging.warning(f"Unknown optimization mode: {optimization_mode}, using 'balanced'")
        optimization_mode = "balanced"
    
    # Get task types for this optimization mode
    io_task_type = task_types[optimization_mode]["io"]
    compute_task_type = task_types[optimization_mode]["compute"]
    
    # Check if output already exists
    exists, output_file = check_output_exists(output_dir, variable, year)
    if exists and not overwrite and not generate_report:
        logging.info(f"Output file {output_file} already exists, skipping (overwrite=False)")
        return
    
    # Initialize client variables outside try block
    io_client = io_cluster = compute_client = compute_cluster = None
    
    try:
        # Configure additional Dask settings to prevent memory issues
        dask.config.set({
            "distributed.worker.memory.spill": 0.85,  # Spill to disk at 85% memory
            "distributed.worker.memory.target": 0.75,  # Target 75% memory usage
            "distributed.worker.memory.pause": 0.95,   # Pause execution at 95% memory
            "distributed.worker.memory.terminate": 0.98  # Terminate at 98% memory
        })
        
        # Set up performance report context
        try:
            perf_file = str(Path.home() / f"{variable}_{year}_performance.html")
            context = performance_report(filename=perf_file) if generate_report else nullcontext()
        except Exception as e:
            logging.warning(f"Could not access home directory for performance report: {e}")
            logging.warning("Falling back to current directory for performance report")
            context = performance_report(filename=f"{variable}_{year}_performance.html") if generate_report else nullcontext()
        
        with context:
            # Get optimal chunk sizes for this variable
            chunks = get_variable_chunks(variable)
            
            # ===== I/O PHASE =====
            logging.info(f"Starting I/O phase with {io_task_type} configuration")
            io_client, io_cluster = get_dask_client(cores, memory_limit, io_task_type)
            
            # Get grid information
            logging.info(f"Retrieving grid information from {geo_file}")
            grid_info = get_grid_info(filepaths[0], geo_file)
            
            # Read input data
            logging.info(f"Reading data for {variable} from {len(filepaths)} files")
            ds = read_data(filepaths, variable, chunks)
            
            # Process the variable - this is mostly I/O and simple aggregation
            logging.info(f"Processing variable {variable}")
            processed_ds = process_variable(ds, variable)
            
            # Clean up intermediate data to free memory
            del ds
            io_client.run(lambda: gc.collect())  # Trigger garbage collection on workers
            
            # Close I/O client
            logging.info("Closing I/O phase Dask client")
            io_client.close()
            io_cluster.close()
            
            # ===== COMPUTATION PHASE =====
            logging.info(f"Starting computation phase with {compute_task_type} configuration")
            compute_client, compute_cluster = get_dask_client(cores, memory_limit, compute_task_type)
            
            # Regrid to EPSG:3338 - computation intensive
            logging.info(f"Reprojecting data to EPSG:3338")
            reprojected_ds = regrid_to_3338(processed_ds, grid_info)
            
            # Clean up intermediate data to free memory
            del processed_ds
            compute_client.run(lambda: gc.collect())  # Trigger garbage collection on workers
            
            # Write output
            logging.info(f"Writing output to {output_file}")
            write_output(reprojected_ds, output_file)
            
            # Clean up final data
            del reprojected_ds
            compute_client.run(lambda: gc.collect())  # Trigger garbage collection on workers
        
        return output_file
    
    except RecursionError as e:
        logging.error(f"RecursionError processing {variable} for year {year}: {e}")
        logging.error("Try increasing dask.config.set({'distributed.worker.memory.sizeof.sizeof-recurse-limit': 100})")
        import traceback
        logging.error(traceback.format_exc())
        return None
    except MemoryError as e:
        logging.error(f"MemoryError processing {variable} for year {year}: {e}")
        logging.error("Try increasing memory_limit in the SLURM script and reducing chunk sizes")
        import traceback
        logging.error(traceback.format_exc())
        return None
    except Exception as e:
        logging.error(f"Error processing {variable} for year {year}: {e}")
        import traceback
        logging.error(traceback.format_exc())
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
            
        logging.info(f"Closed all Dask clients and clusters")


# Add nullcontext for use in performance report
from contextlib import nullcontext


def main() -> None:
    """Main processing function."""
    # Parse and validate arguments
    args = parse_args()
    setup_logging(args.verbose)
    
    # Update recursion limit if specified
    if args.recurse_limit != 50:
        logging.info(f"Setting Dask recursion limit to {args.recurse_limit}")
        dask.config.set({"distributed.worker.memory.sizeof.sizeof-recurse-limit": args.recurse_limit})
    
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
            generate_report=args.generate_report,
            cores=args.cores,
            memory_limit=args.memory_limit,
            optimization_mode=args.optimization_mode,
            overwrite=args.overwrite
        )
        
        if result:
            logging.info(f"Successfully processed {variable} for year {year}")
            sys.exit(0)
        else:
            logging.error(f"Failed to process {variable} for year {year}")
            sys.exit(1)
    finally:
        stop_mem_monitor()


if __name__ == "__main__":
    main() 