# ERA5 Processing Pipeline

This pipeline is for processing ERA5 4 km WRF-downscaled data. The pipeline is intended to resample the input data source from hourly to daily resolution, and put it on the EPSG:3338 grid. This approach uses Dask for parallel processing within individual compute nodes, but processes one variable and one year at a time using SLURM for job management.

## Project Structure

### Main Components

- `process_single_variable.py`: Core processing script that handles one variable/year
- `process_era5_variable.sbatch`: SLURM job template for individual processing jobs
- `submit_era5_jobs.py`: Script for submitting multiple jobs
- `era5_variables.py`: Variable definitions and metadata, aggregation functions
- `config.py`: Configuration settings and environment variable handling

### Utilities

There are several modules in the `utils` directory:
- **dask_utils.py**: Manages Dask client configuration and distributed computing setup
- **logging.py**: Provides standardized logging configuration
- **custom_agg_funcs.py**: Bespoke functions for aggregating data where a simple lambda is awkward.

### Quality Control

The `qc` directory: quality control plotting functions and notebooks.

## Configuration

The pipeline uses environment variables for configuration, with sensible defaults for the Chinook environment. You can set these variables to override the defaults:

### Required Environment Variables
- `ERA5_INPUT_DIR`: Input directory containing ERA5 data (default: "/beegfs/CMIP6/wrf_era5/04km")
- `ERA5_OUTPUT_DIR`: Output directory for processed files (default: "/beegfs/CMIP6/cparr4/daily_downscaled_era5_for_rasdaman")
- `ERA5_GEO_FILE`: Path to WRF geo_em file (default: "/beegfs/CMIP6/wrf_era5/geo_em.d02.nc")

### Optional Environment Variables
- `ERA5_START_YEAR`: Start year for processing (default: 1960)
- `ERA5_END_YEAR`: End year for processing (default: 2020)
- `ERA5_DATA_VARS`: Comma-separated list of variables to process (default: t2_mean,t2_min,t2_max)
- `ERA5_BATCH_SIZE`: Number of files to process per batch (default: 90, range: 2-365)
- `ERA5_INPUT_PATTERN`: File pattern for input files (default: "era5_wrf_dscale_4km_{date}.nc")
- `ERA5_DASK_CORES`: Number of cores to use (default: auto-detect)
- `ERA5_DASK_MEMORY_LIMIT`: Memory limit for Dask workers (default: 64GB)
- `ERA5_DASK_TASK_TYPE`: Task type for Dask workers (default: io_bound)

## Execution

The pipeline should be executed on Chinook, and can be launched from a "login" or "debug" node.

### `submit_era5_jobs.py`: Processing Multiple Variables and Years

The job submission script is the recommended method of executing this pipeline. For example:

```bash
# Process multiple variables for a range of years
python submit_era5_jobs.py --variables t2_mean,t2_min,t2_max --start_year 1990 --end_year 2000
```

#### Job Submission Options

The job submission script provides several options:

- `--variables`: Comma-separated list of variables to process (required)
- `--start_year`: Start year for processing (default from ERA5_START_YEAR)
- `--end_year`: End year for processing (default from ERA5_END_YEAR)
- `--max_concurrent`: Maximum number of concurrent jobs (default: 30)
- `--overwrite`: Force reprocessing of existing files (default: False)
- `--no_retry`: Disable automatic retry of timed-out jobs (default: False)

### `process_era5_variable.sbatch`: Processing a Single Variable for a Single Year

This is **not** the recommended entry point for the pipeline, but it is useful for rapid testing and for isolating the sbatch template and worker script from the job submission script. To manually process a single variable for a single year:

```bash
# Basic usage
sbatch process_era5_variable.sbatch <year> <variable>

# Force overwrite of existing files
sbatch process_era5_variable.sbatch 1980 t2_mean overwrite
```

The SLURM script accepts up to three parameters:
1. `year`: Year to process (required)
2. `variable`: Variable to process (required)
3. `overwrite`: Force reprocessing of existing files (optional)

The SLURM script will pass those parameters to the worker script.

### `process_single_variable.py`: The Worker Script

This is **not** the recommended entry point to this pipeline. However, directly executing the script is useful for testing and isolating the core logic from any and all SLURM orchestration. The worker script has options for fine-tuning the data processing:

- `--year`: Year to process (required)
- `--variable`: Variable to process (required)
- `--overwrite`: Overwrite existing output files (default: False)

Dask configuration (cores, memory limits, etc.) is handled through environment variables via the centralized config system. For example:

```bash
# Basic usage
python process_single_variable.py --year 1980 --variable t2_mean

# With custom Dask configuration via environment variables
export ERA5_DASK_CORES=12
export ERA5_DASK_MEMORY_LIMIT="64GB"
python process_single_variable.py --year 1980 --variable t2_mean
```

## Monitoring Progress

Job logs are written to the `logs/era5_process/` directory with one SLURM log file per variable/year combination. The pipeline uses console-only logging to eliminate redundant log files - all output is captured in the SLURM log files.

To check the status of your job submissions:

```bash
watch squeue --me
```

To tail the logs:

```bash
tail -f logs/era5_process/variable_id/*.out
```

Logs will get wiped at the start of each run, so if you want past logs to persist, move them elsewhere.

## Automatic Retry System

The pipeline includes an automatic retry system, handled directly within the `submit_era5_jobs.py` script. This system detects and resubmits failed jobs without manual intervention, which is particularly useful for handling occasional SLURM timeouts or transient failures.

### How It Works

When you run `submit_era5_jobs.py` (and if the `--no_retry` flag is not used):

1.  After the initial batch of jobs is submitted and completes, the script waits for all queued jobs to finish.
2.  **Scans SLURM log files**: It then inspects the `.out` log files located in `logs/era5_process/<variable_name>/` for SLURM timeout messages (specifically, lines containing "CANCELLED AT" and "DUE TO TIME LIMIT").
3.  **Resubmits timed-out jobs**: Any job identified as having timed out is automatically resubmitted using the same sbatch command.
4.  **Final Validation**: After any retries, a validation step checks for the existence and integrity of the expected output NetCDF files.

This retry mechanism is part of the main workflow in `submit_era5_jobs.py` and does not use a separate tracking file or maintain complex retry state beyond what is observable in the SLURM logs and the script's own logging.

### Example Output

When `submit_era5_jobs.py` initiates a retry for a timed-out job, you will see log messages similar to this:

```
INFO - Timeout detected for t2_mean year 1978
INFO - RETRY: Submitting t2_mean 1978
INFO - RETRY: Submitted job <job_id> for t2_mean year 1978
```

## Output Specs

The processed data will be saved in the specified output directory with the following structure:

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
- CF compliant where possible

## Troubleshooting

- **Processing is slow**: Adjust the chunk sizes or batch sizes, or increase CPU cores via `ERA5_DASK_CORES` environment variable
- **Too many jobs time out**: Increase the time limit in the sbatch script
- **Too many jobs in queue**: Decrease the `--max_concurrent` parameter
- **Missing variable**: Check that the variable name is correct and exists in `era5_variables.py`
- **RecursionError**: Increase the recursion limit in the `dask_utils` parameter module
- **OOM killed by SLURM**: Increase memory allocation in sbatch script or reduce chunk sizes, or adjust `ERA5_DASK_MEMORY_LIMIT` environment variable

# ERA5 Pipeline Default Configuration Values

| Configuration Variable | Default Value | Source of Default | Description |
|------------------------|---------------|---------|-------------|
| `ERA5_INPUT_DIR` | `/beegfs/CMIP6/wrf_era5/04km` | Hardcoded | Input directory containing ERA5 data |
| `ERA5_OUTPUT_DIR` | `/beegfs/CMIP6/$USER/daily_downscaled_era5_for_rasdaman` | Hardcoded | Output directory for processed files |
| `ERA5_GEO_FILE` | `/beegfs/CMIP6/wrf_era5/geo_em.d02.nc` | Hardcoded | Path to WRF geo_em file |
| `ERA5_FILE_PATTERN` | `era5_wrf_dscale_4km_{date}.nc` | Hardcoded | File pattern for input files |
| `ERA5_START_YEAR` | `1960` | Hardcoded | Start year for processing |
| `ERA5_END_YEAR` | `2020` | Hardcoded | End year for processing |
| `ERA5_DATA_VARS` | `t2_mean,t2_min,t2_max` | Hardcoded | Default variables to process |
| `ERA5_BATCH_SIZE` | `90` | Hardcoded | Files per batch (range: 2-365) |
| `ERA5_DASK_CORES` | Auto-detect | Environment | Number of cores for Dask workers |
| `ERA5_DASK_MEMORY_LIMIT` | `64GB` | Environment | Memory limit for Dask workers |
| `ERA5_DASK_TASK_TYPE` | `io_bound` | Environment | Task type for Dask optimization |

## Performance Optimization

The default configuration values have been optimized based on comprehensive performance profiling. The pipeline now uses:

- **Batch Size**: 90 files per batch (23% faster than previous 365-file default)
- **Task Type**: io_bound Dask configuration (15% faster than balanced default)
- **Memory Limit**: 64GB allocation (17% faster than previous 85GB default)

These optimizations provide approximately **40-45% performance improvement** over previous defaults.

### Dask Memory Configuration

The `ERA5_DASK_MEMORY_LIMIT` parameter controls total memory allocation for Dask workers:

- **64GB** (default): Optimized allocation based on performance profiling
- **Performance advantage**: 17% faster than 85GB (157.2s vs 190.3s average)
- **Resource efficiency**: 21GB less memory per job, enabling higher cluster throughput
- **Safety margin**: 33% buffer below SLURM allocation (96GB) vs previous 11%

**Memory Performance Results**:
- 64GB: 157.2s average (fastest, optimal resource usage)
- 85GB: 190.3s average (+21% slower, over-allocated)

### Dask Task Type Configuration

The `ERA5_DASK_TASK_TYPE` parameter controls how Dask optimizes worker allocation:

- **io_bound** (default): Optimized for I/O operations, more workers with fewer threads per worker
- **balanced**: Balanced approach for mixed workloads
- **compute_bound**: Optimized for CPU-intensive tasks, fewer workers with more threads

**Performance Results**:
- io_bound: 147.2s average (fastest)
- balanced: 173.9s average (+18% slower)
- compute_bound: 271.4s average (+84% slower)

### Batch Size Configuration

The `ERA5_BATCH_SIZE` parameter controls how many files are processed together in memory:

- **Valid Range**: 2-365 files
- **Optimal Range**: 90-180 files (based on profiling results)
- **Default**: 90 files (optimal for most ERA5 workloads)

**When to Adjust**:
- **Keep default (90)** for optimal performance
- **Use 180** for slightly larger batch processing
- **Use smaller batches (30-60)** only if memory constrained or for 3D variables