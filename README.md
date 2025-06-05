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

### Derived Configuration Path
**WRF `geo_em` File Path**

The path to the WRF projection file (`geo_em.d02.nc`) is no longer configured via an environment variable. It is now automatically derived from the `ERA5_INPUT_DIR` path.

**Assumption:** The pipeline critically assumes that the `geo_em.d02.nc` file is located exactly one directory level above the `ERA5_INPUT_DIR`.

For example, if `ERA5_INPUT_DIR` is set to `/beegfs/CMIP6/wrf_era5/04km`, the pipeline will automatically look for the geo file at `/beegfs/CMIP6/wrf_era5/geo_em.d02.nc`. The processing will fail if this file is not found at the derived location.

**Input File Naming Convention**

Similarly, the pattern for input data files is no longer configurable and is assumed to be `era5_wrf_dscale_4km_{date}.nc`, where `{date}` is the date string (e.g., `1980-01-01`).

### Optional Environment Variables
- `ERA5_START_YEAR`: Start year for processing (default: 1960)
- `ERA5_END_YEAR`: End year for processing (default: 2020)
- `ERA5_DATA_VARS`: Comma-separated list of variables to process (default: t2_mean,t2_min,t2_max)
- `ERA5_BATCH_SIZE`: Number of files to process per batch (default: 90, range: 2-365)
- `ERA5_DASK_CORES`: Number of cores Dask should use. If set, this overrides auto-detection. Auto-detection prioritizes `SLURM_CPUS_PER_TASK` if in a SLURM environment, otherwise `os.cpu_count()`.
- `ERA5_DASK_TASK_TYPE`: Task type for Dask worker optimization (default: `io_bound`, set in `config.py`). Can be set to `io_bound`, `compute_bound`, or `balanced`. This environment variable overrides the default.

### Dask Configuration Details

Understanding how Dask is configured for parallelism and memory is crucial for efficient processing.

**Cores:**
The number of cores Dask utilizes is determined in the following order of precedence:
1.  The `ERA5_DASK_CORES` environment variable, if set.
2.  The `SLURM_CPUS_PER_TASK` environment variable, if the job is running within a SLURM environment.
3.  The total number of CPU cores available on the node, as reported by `os.cpu_count()`.

**Memory:**
The total memory available to the Dask `LocalCluster` is determined in two steps:
1.  **Total Available Memory**: The pipeline first determines the total memory available to the job. This is either the full amount allocated by SLURM (via the `SLURM_MEM_PER_NODE` environment variable) or a fallback of "64GB" if running outside a SLURM environment.
2.  **Dask Worker Pool Allocation**: The Dask worker pool is **always** configured to use **90%** of this total available memory. The remaining 10% is reserved as a safety buffer for the operating system and other overhead.

This 90% fraction is a fixed heuristic and does not change based on the `task_type`. It is **critical** to ensure that the memory requested from SLURM (via `#SBATCH --mem`) is sufficient. For a robust setup, it is recommended to request at least 96GB.

The `ERA5_DASK_TASK_TYPE` (defaulting to `io_bound`) influences how Dask allocates workers and threads based on the available cores, but it does not affect memory allocation.

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
# Note: ERA5_DASK_MEMORY_LIMIT is not used by the current Dask setup.
# Dask memory is auto-configured based on SLURM allocation or an internal default.
# Ensure your SLURM --mem request is ~15-25% higher than Dask's expected usage.
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
- **OOM killed by SLURM**: Increase memory allocation in the sbatch script (`#SBATCH --mem`). Dask automatically uses a portion (typically 90%) of this. Ensure the SLURM allocation is sufficiently higher (15-25%) than Dask's expected peak usage. Reducing batch size (`ERA5_BATCH_SIZE`) or, for very large datasets, considering a reduction in Dask cores (`ERA5_DASK_CORES`) to decrease memory pressure per core might also help.

# ERA5 Pipeline Default Configuration Values

| Configuration Variable | Default Value | Source of Default | Description |
|------------------------|---------------|---------|-------------|
| `ERA5_INPUT_DIR` | `/beegfs/CMIP6/wrf_era5/04km` | Hardcoded | Input directory containing ERA5 data |
| `ERA5_OUTPUT_DIR` | `/beegfs/CMIP6/$USER/daily_downscaled_era5_for_rasdaman` | Hardcoded | Output directory for processed files |
| `ERA5_START_YEAR` | `1960` | Hardcoded | Start year for processing |
| `ERA5_END_YEAR` | `2020` | Hardcoded | End year for processing |
| `ERA5_DATA_VARS` | `t2_mean,t2_min,t2_max` | Hardcoded | Default variables to process |
| `ERA5_BATCH_SIZE` | `90` | Hardcoded | Files per batch (range: 2-365) |
| `ERA5_DASK_CORES` | Auto-detect | Code/Auto-detect (via SLURM or `os.cpu_count()`); Env var overrides. | Number of cores for Dask workers |
| `ERA5_DASK_TASK_TYPE` | `io_bound` | Code (defaults to `io_bound` in `config.py`); Env var overrides. | Task type for Dask optimization |

## Performance Optimization

The default configuration values have been optimized based on comprehensive performance profiling. The pipeline now uses:

- **Batch Size**: 90 files per batch (23% faster than previous 365-file default)
- **Task Type**: io_bound Dask configuration (15% faster than balanced default)
- **Memory Allocation**: Dask automatically configures its memory. Optimal performance was observed when SLURM jobs were allocated memory such that Dask used approximately 64GB. (e.g. SLURM allocated ~72GB, Dask used ~90% of that).

### Dask Memory Configuration

Dask's total memory for its worker pool is configured to use a fixed fraction (**90%**) of the total memory available to the job. The total memory is determined by the SLURM allocation (`#SBATCH --mem`) or an internal 64GB default if not in a SLURM environment.

For example, the `process_era5_variable.sbatch` script now requests 96GB from SLURM. Dask will see this 96GB as the total available memory and allocate its worker pool with 90% of it, which is approximately 86GB. This leaves a robust ~10GB buffer for system overhead. This approach is simpler and more stable than previous configurations.

### Dask Task Type Configuration

The `ERA5_DASK_TASK_TYPE` parameter controls how Dask optimizes the cluster's topology. It only affects the number of workers and the number of threads per worker; it does not change the memory allocation strategy.

- **io_bound** (default): Optimized for I/O operations, more workers with fewer threads per worker
- **balanced**: Balanced approach for mixed workloads
- **compute_bound**: Optimized for CPU-intensive tasks, fewer workers with more threads

### Batch Size Configuration

The `ERA5_BATCH_SIZE` parameter controls how many files are processed together in memory:

- **Valid Range**: 2-365 files
- **Optimal Range**: 90-180 files (based on profiling results)
- **Default**: 90 files (optimal for most ERA5 workloads)

**When to Adjust**:
- **Keep default (90)** for optimal performance
- **Use 180** for slightly larger batch processing
- **Use smaller batches (30-60)** only if memory constrained or for 3D variables