# ERA5 Processing Pipeline

This is the current, simplified approach to processing ERA5 downscaled data. This approach uses Dask for parallel processing within nodes but processes one variable and one year at a time using SLURM for job management.

## Overview

The simplified approach consists of:

1. A main processing script (`process_single_variable.py`) that processes one variable for one year
2. A job submission script (`submit_era5_jobs.py`) that generates and submits SLURM jobs
3. A template SBATCH script (`process_era5_variable.sbatch`) for individual job submissions

This approach is designed to be simpler and more reliable than the distributed Dask approach, with clearer error handling and easier debugging.


## Project Structure

The main components of the pipeline are:

- `process_single_variable.py`: Core processing script that handles one variable/year
- `process_era5_variable.sbatch`: SLURM job template for individual processing jobs
- `submit_era5_jobs.py`: Script for submitting multiple jobs
- `era5_variables.py`: Variable definitions and metadata
- `config.py`: Configuration settings

## Processing a Single Variable for a Single Year

To process a single variable for a single year manually:

```bash
# Basic usage
sbatch process_era5_variable.sbatch <year> <variable>

# Example
sbatch process_era5_variable.sbatch 1980 t2_mean

# Force overwrite of existing files, handy for testing
sbatch process_era5_variable.sbatch 1980 t2_mean overwrite
```

The SLURM script accepts up to three parameters:
1. `year`: Year to process (required)
2. `variable`: Variable to process (required)
3. `overwrite`: Set to `overwrite` to force reprocessing of existing files (optional)


## Processing Multiple Variables and Years

To process multiple variables and years, use the `submit_era5_jobs.py` script:

```bash
# Process a single variable for a range of years
python submit_era5_jobs.py --variable t2_mean --start_year 1980 --end_year 1985

# Process multiple variables for a range of years
python submit_era5_jobs.py --variables t2_mean,t2_min,t2_max --start_year 1990 --end_year 2000

# Process all available variables for a single year
python submit_era5_jobs.py --all_variables --start_year 2000 --end_year 2000
```

### Job Submission Options

The job submission script provides several options for controlling job submission:

- `--max_concurrent`: Maximum number of concurrent jobs (default: 20)
- `--output_dir`: Output directory (default from config)

## Advanced Command Line Options

The `process_single_variable.py` script provides additional options for fine-tuning processing:

### Input/Output Options

- `--input_dir`: Directory containing ERA5 data (default from config)
- `--output_dir`: Directory for output files (default from config)
- `--geo_file`: Path to WRF geo_em file for projection information (default from config)
- `--fn_str`: File pattern for input files (default from config)
- `--overwrite`: Overwrite existing output files (default: False)

### Performance Options

- `--cores`: Number of cores to use (default: auto-detect)
- `--memory_limit`: Memory limit for Dask workers (default: 16GB)

Example with advanced options:

```bash
python process_single_variable.py \
    --year 1980 \
    --variable t2_mean \
    --cores 24 \
    --memory_limit "85GB"
```

## Configuration and Environment Variables

Many default values come from `config.py`, which reads from environment variables with sensible defaults. The following environment variables can be set to override defaults:

- `ERA5_INPUT_DIR`: Input directory (default: "/beegfs/CMIP6/wrf_era5/04km")
- `ERA5_OUTPUT_DIR`: Output directory (default: "/beegfs/CMIP6/cparr4/daily_downscaled_era5_for_rasdaman")
- `ERA5_START_YEAR`: Default start year (default: 1980)
- `ERA5_END_YEAR`: Default end year (default: 1990)
- `ERA5_DATA_VARS`: Comma-separated list of variables to process
- `ERA5_INPUT_PATTERN`: File pattern for input files (default: "era5_wrf_dscale_4km_{date}.nc")
- `ERA5_OUTPUT_TEMPLATE`: Template for output files (default: "era5_wrf_dscale_4km_{datavar}_{year}_3338.nc")
- `GEO_EM_FILE`: Path to WRF geo_em file (default: "/beegfs/CMIP6/wrf_era5/geo_em.d02.nc")

## Monitoring Progress

To check the status of your jobs:

```bash
watch squeue --me
```

## Understanding the Output

The processed data will be saved in the specified output directory (default: from config.py) with the following structure:

```
<output_dir>/
  <variable1>/
    <variable1>_<year>_era5_4km_3338.nc
    ...
  <variable2>/
    <variable2>_<year>_era5_4km_3338.nc
    ...
  ...
```

Each NetCDF file contains the processed data for one variable and one year, with the following characteristics:

- Daily temporal resolution (aggregated from hourly data)
- 4km spatial resolution
- EPSG:3338 (Alaska Albers) projection
- Compression applied for reduced file size

## Customizing the Processing

To customize the processing:

1. Edit `process_single_variable.py` to modify the core processing logic
2. Edit `process_era5_variable.sbatch` to adjust resource requests (memory, CPU cores, time)
3. Edit `submit_era5_jobs.py` to change job submission behavior
4. Modify environment variables or edit `config.py` to change default configurations

### Input File Pattern

The input file pattern uses the `{date}` placeholder which is automatically expanded to include wildcards when searching for files. For example, with the default pattern `era5_wrf_dscale_4km_{date}.nc` and a specific year (e.g., 1980), the script looks for files matching `era5_wrf_dscale_4km_1980-*.nc` in the year's directory.

## Memory Management and Optimization

The pipeline includes several features for managing memory and optimizing performance:

### Memory Monitoring

Memory monitoring is now always enabled and logs memory usage every 5 seconds. This provides continuous visibility into memory usage throughout the processing pipeline, which can help identify memory bottlenecks without requiring any additional configuration.

## Troubleshooting

Common issues and solutions:

- **Job fails with out-of-memory error**: Increase the memory in the sbatch script or adjust chunk sizes in `process_single_variable.py`
- **Processing is slow**: Adjust the chunk sizes or increase the CPU cores
- **Job times out**: Increase the time limit in the sbatch script
- **Too many jobs in queue**: Decrease the `--max_concurrent` parameter
- **Missing variable**: Check that the variable name is correct and exists in `era5_variables.py`
- **RecursionError**: Increase the recursion limit in the `dask_utils` parameter module
- **OOM killed by SLURM**: Increase memory allocation in sbatch script or reduce chunk sizes

Memory monitoring logs can help identify where memory usage spikes. 