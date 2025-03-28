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
sbatch process_era5_variable.sbatch 1980 t2_mean
```

This will submit a SLURM job that processes the `t2_mean` variable for the year 1980.

### Optimization Modes

The processing script supports different optimization modes for various workloads:

- `io_optimized` (default): Optimizes for I/O operations with more workers and less memory per worker
- `compute_optimized`: Optimizes for computation with fewer workers and more threads per worker
- `balanced`: Uses a balanced approach for mixed workloads
- `fully_optimized`: Uses I/O optimization for file operations and compute optimization for computational tasks

To specify a different optimization mode:

```bash
python process_single_variable.py --year 1980 --variable t2_mean --optimization_mode compute_optimized
```

## Processing Multiple Variables and Years

To process multiple variables and years, use the `submit_era5_jobs.py` script:

```bash
# Process a single variable for a range of years
python submit_era5_jobs.py --variable t2_mean --start_year 1980 --end_year 1985

# Process multiple variables for a range of years
python submit_era5_jobs.py --variables t2_mean,t2_min,t2_max --start_year 1990 --end_year 2000

# Process all available variables for a single year
python submit_era5_jobs.py --all_variables --start_year 2000 --end_year 2000

# Process with a specific optimization mode
python submit_era5_jobs.py --variable t2_mean --start_year 1980 --end_year 1985 --optimization_mode compute_optimized
```

### Job Submission Options

The job submission script provides several options for controlling job submission:

- `--max_concurrent`: Maximum number of concurrent jobs (default: 20)
- `--optimization_mode`: Optimization mode for worker configuration (choices: balanced, io_optimized, compute_optimized, fully_optimized; default: io_optimized)
- `--use_job_arrays`: Use SLURM job arrays for years (one array per variable)
- `--no_submit`: Generate job scripts but don't submit them
- `--output_dir`: Output directory (default from config)
- `--wait_time`: Wait time in seconds between job submissions (default: 2)
- `--verbose`: Enable verbose logging

## Monitoring Progress

To check the status of your jobs:

```bash
squeue -u $USER
```

To cancel all your jobs:

```bash
scancel -u $USER
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

## Troubleshooting

Common issues and solutions:

- **Job fails with out-of-memory error**: Increase the memory in the sbatch script or adjust chunk sizes in `process_single_variable.py`
- **Processing is slow**: Adjust the chunk sizes or increase the CPU cores
- **Job times out**: Increase the time limit in the sbatch script
- **Too many jobs in queue**: Decrease the `--max_concurrent` parameter or use job arrays
- **Missing variable**: Check that the variable name is correct and exists in `era5_variables.py`

For more detailed diagnostic information, check the Dask performance reports that are generated when using the `--generate_report` flag. 