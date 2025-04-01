# ERA5 Processing Pipeline

This pipeline is for processing the ERA5 4 km WRF-downscaled data. The pipeline is intended to resample the input data source from hourly to daily resolution, and put it on the EPSG:3338 grid. This approach uses Dask for parallel processing within individual compute nodes, but processes one variable and one year at a time using SLURM for job management.

## Overview

After some experimentation the code is settling on a simplified approach consisting of:

1. A main processing worker script (`process_single_variable.py`) that processes one variable for one year
2. A job submission script (`submit_era5_jobs.py`) that generates and submits SLURM jobs
3. A template SBATCH script (`process_era5_variable.sbatch`) for individual job submissions

## Project Structure

The main components of the pipeline are:

- `process_single_variable.py`: Core processing script that handles one variable/year
- `process_era5_variable.sbatch`: SLURM job template for individual processing jobs
- `submit_era5_jobs.py`: Script for submitting multiple jobs
- `era5_variables.py`: Variable definitions and metadata
- `config.py`: Configuration settings

## Processing a Single Variable for a Single Year

This isn't really a recommended entry point for the pipeline, but it can be useful for rapid testing and isolating the .sbatch and worker script behavior from the job submission script. To process a single variable for a single year manually:

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
3. `overwrite`: Just include `overwrite` to force reprocessing of existing files (optional)


## Processing Multiple Variables and Years

To process multiple variables and years, use the `submit_era5_jobs.py` script:

```bash
# Process a single variable for a range of years
python submit_era5_jobs.py --variable t2_mean --start_year 1980 --end_year 1985

# Process multiple variables for a range of years
python submit_era5_jobs.py --variables t2_mean,t2_min,t2_max,rh2_mean --start_year 1990 --end_year 2000

# NOT WORKING YET Process all available variables for a single year NOT WORKING YET
python submit_era5_jobs.py --all_variables --start_year 2000 --end_year 2000
```

### Job Submission Options

The job submission script provides several options for controlling job submission:

- `--max_concurrent`: Maximum number of concurrent jobs (default: 20). Nothing wrong with going 30+ here.
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

Example with advanced options - again, not really the recommended "production" entry point to this pipeline, but useful for testing and isolating the core logic from the orchestration:

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
- `ERA5_START_YEAR`: Default start year (default: 1960)
- `ERA5_END_YEAR`: Default end year (default: 2020)
- `ERA5_DATA_VARS`: Comma-separated list of variables to process (default: t2 min/mean/max trio)
- `ERA5_INPUT_PATTERN`: File pattern for input files (default: "era5_wrf_dscale_4km_{date}.nc")
- `GEO_EM_FILE`: Path to WRF geo_em file (default: "/beegfs/CMIP6/wrf_era5/geo_em.d02.nc")

## Monitoring Progress
A bunch of log files will get written to the working directory, categorized by variable. The logging here isn't ultra-tidy because there is overlap between the python log files (`*.log`) and the SLURM log files (`*.out`).

To check the status of your job submissions:

```bash
watch squeue --me
```

To tail the logs:

```bash
tail -f logs/variable_id/*.out
```

The link to the client dashboard is written to the log - but I actually don't reccommend forwarding/using the dashboard to monitor progress at this point because the pipeline executes rapidly enough that it isn't very useful.

## Understanding the Output

The processed data will be saved in the specified output directory (default: from config.py) with the following structure:

```
<output_dir>/
  <variable1>/
    <variable1>_<year>_daily_era5_4km_3338.nc
    ...
  <variable2>/
    <variable2>_<year>_daily_era5_4km_3338.nc
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

### Memory Monitoring

Memory monitoring logs memory usage every 5 seconds. This provides continuous visibility into memory usage throughout the processing pipeline, which can help identify memory bottlenecks without requiring any additional configuration.

### Troubleshooting

Common issues and solutions:

- **Job fails with out-of-memory error**: Increase the memory in the sbatch script or adjust chunk sizes in `process_single_variable.py`
- **Processing is slow**: Adjust the chunk sizes or increase the CPU cores
- **Job times out**: Increase the time limit in the sbatch script
- **Too many jobs in queue**: Decrease the `--max_concurrent` parameter
- **Missing variable**: Check that the variable name is correct and exists in `era5_variables.py`
- **RecursionError**: Increase the recursion limit in the `dask_utils` parameter module
- **OOM killed by SLURM**: Increase memory allocation in sbatch script or reduce chunk sizes
