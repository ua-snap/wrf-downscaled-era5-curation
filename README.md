# ERA5 Processing Pipeline

This pipeline is for processing ERA5 4 km WRF-downscaled data. The pipeline is intended to resample the input data source from hourly to daily resolution, and put it on the EPSG:3338 grid. This approach uses Dask for parallel processing within individual compute nodes, but processes one variable and one year at a time using SLURM for job management.

## Project Structure

### Main Components

- `process_single_variable.py`: Core processing script that handles one variable/year
- `process_era5_variable.sbatch`: SLURM job template for individual processing jobs
- `submit_era5_jobs.py`: Script for submitting multiple jobs
- `era5_variables.py`: Variable definitions and metadata, aggregation functions
- `config.py`: Configuration settings

### Utilities

There are several modules in the `utils` directory:
- **dask_utils.py**: Manages Dask client configuration and distributed computing setup
- **memory.py**: Tracks memory usage during processing
- **logging.py**: Provides standardized logging configuration

## Execution

### Processing a Single Variable for a Single Year

This is **not** the recommendeded entry point for the pipeline, but it is useful for rapid testing and for isolating the sbatch template and worker script from the job submission script. To manually process a single variable for a single yea:

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
3. `overwrite`: Force reprocessing of existing files (optional)

### Processing Multiple Variables and Years

Using the `submit_era5_jobs.py` is the recommended method of executing this pipeline for multiple variables and years:

```bash
# Process a single variable for a range of years
python submit_era5_jobs.py --variable t2_mean --start_year 1980 --end_year 1985

# Process multiple variables for a range of years
python submit_era5_jobs.py --variables t2_mean,t2_min,t2_max,rh2_mean --start_year 1990 --end_year 2000

# NOT WORKING YET Process all variables for a single year
python submit_era5_jobs.py --all_variables --start_year 2000 --end_year 2000
```

#### More Job Submission Options

The job submission script provides several additional options:

- `--max_concurrent`: Maximum number of concurrent jobs (default: 20). Nothing wrong with going 30+ here, depending on how greedy you want to be in terms of grabbing `t2small` nodes.
- `--output_dir`: Output directory (default from config). You can set this with an environment variable as well. We may deprecate in this future such that the output directory may only be set by providing the environment variable to simply the configuration pathways.
- `--overwrite`: Boolean flag to overwrite existing output files (default: False)

#### More Worker Script Options

The `process_single_variable.py` worker script has some additional options for fine-tuning the data processing:

- `--input_dir`: Directory containing ERA5 data (default from config)
- `--output_dir`: Directory for output files (default from config)
- `--geo_file`: Path to WRF geo_em file for projection information (default from config)
- `--fn_str`: File pattern for input files (default from config)
- `--overwrite`: Overwrite existing output files (default: False)
- `--cores`: Number of cores to use (default: auto-detect)
- `--memory_limit`: Memory limit for Dask workers (default: 16GB)

These arguments can be set explicitly when doing development, but otherwise they are set with defaults or are passed down via the job submission script. Here is an example, but again, this is **not** the recommended entry point to this pipeline. However, this pathway is useful for testing and isolating the core logic from any and all SLURM orchestration:

```bash
python process_single_variable.py \
    --year 1980 \
    --variable t2_mean \
    --cores 24 \
    --memory_limit "85GB"
```

Some of these options may be deprecated in the future to reduce the overall configuration surface area.

## Configuration and Environment Variables

Default values are generated via `config.py` reading environment variables, and an attempt has been made to provide sensible defaults assuming a Chinook BEEGFS file system. The following environment variables can be set to override defaults:

- `ERA5_INPUT_DIR`: Input directory (default: "/beegfs/CMIP6/wrf_era5/04km")
- `ERA5_OUTPUT_DIR`: Output directory (default: "/beegfs/CMIP6/cparr4/daily_downscaled_era5_for_rasdaman")
- `ERA5_START_YEAR`: Start year (default: 1960)
- `ERA5_END_YEAR`: End year (default: 2020)
- `ERA5_DATA_VARS`: Comma-separated list of variables to process (default: t2 min/mean/max trio)
- `ERA5_INPUT_PATTERN`: File pattern for input files (default: "era5_wrf_dscale_4km_{date}.nc")
- `GEO_EM_FILE`: Path to WRF geo_em file (default: "/beegfs/CMIP6/wrf_era5/geo_em.d02.nc")

## Monitoring Progress

One log file per variable/year is written to the `logs` directory.There is also a log for the job submission. Note that the logging here isn't ultra-tidy because there is duplication of content between the python log files (`logs/$variable_id/*.log`) and the SLURM log files (`era5_process/$variable_id/*.out`). But, this is OK for now because we want logging when debugging (just running the Python worker script with no SLURM in the loop) and when scheduling with SLURM. In the SLURM log files you'll see which node is being used for the computation.

To check the status of your job submissions:

```bash
watch squeue --me
```

To tail the logs:

```bash
tail -f logs/variable_id/*.out
```

The link to the Dask client dashboard is written to the log - but port forwarding and using the dashboard to monitor progress isn't very useful because the individual jobs execute rather quickly.

There is a memory monitoring utility (`utils/memory.py`) that logs memory usage every few seconds. The spirit of this endeavor is to have continuous visibility into memory usage throughout the processing pipeline to help identify bottlenecks if/when Dask goes off the rails.

## Output Specs

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

- Daily frequency (aggregated from hourly data)
- 4 km spatial resolution
- EPSG:3338 Alaska Albers projection
- Mode compression applied for reduced file size
- CF compliant (mostly)

## Troubleshooting

Common issues and solutions:

- **Job fails with out-of-memory error**: Increase the memory in the sbatch script or adjust chunk sizes in `process_single_variable.py`
- **Processing is slow**: Adjust the chunk sizes or increase the CPU cores
- **Job times out**: Increase the time limit in the sbatch script
- **Too many jobs in queue**: Decrease the `--max_concurrent` parameter
- **Missing variable**: Check that the variable name is correct and exists in `era5_variables.py`
- **RecursionError**: Increase the recursion limit in the `dask_utils` parameter module
- **OOM killed by SLURM**: Increase memory allocation in sbatch script or reduce chunk sizes
