#!/usr/bin/env python3
"""Generate and submit SLURM jobs for processing ERA5 variables.

This script generates SLURM job submissions for processing ERA5 variables for
specified years. It provides options for controlling the number of concurrent
jobs and generating job arrays where appropriate.

Example usage:
    # Process t2_mean for years 1980-1985
    python submit_era5_jobs.py --variable t2_mean --start_year 1980 --end_year 1985
    
    # Process multiple variables for a range of years
    python submit_era5_jobs.py --variables t2_mean,t2_min,t2_max --start_year 1990 --end_year 2000
    
    # Process all variables for a specific year
    python submit_era5_jobs.py --all_variables --start_year 2000 --end_year 2000
"""

# Standard library imports
import argparse
import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional, Set, Tuple, Dict, Any

# Local imports
from era5_variables import era5_datavar_lut, list_all_variables
from config import config


def setup_logging(verbose: bool = False) -> None:
    """Set up logging configuration.
    
    Args:
        verbose: Whether to use verbose logging
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()]
    )


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.
    
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(description=__doc__)
    
    # Variable selection (mutually exclusive)
    var_group = parser.add_mutually_exclusive_group(required=True)
    var_group.add_argument(
        "--variable",
        type=str,
        help="Single variable to process"
    )
    var_group.add_argument(
        "--variables",
        type=str,
        help="Comma-separated list of variables to process"
    )
    var_group.add_argument(
        "--all_variables",
        action="store_true",
        help="Process all available variables"
    )
    
    # Year range
    parser.add_argument(
        "--start_year",
        type=int,
        default=config.START_YEAR,
        help=f"Start year for processing (default: {config.START_YEAR})"
    )
    parser.add_argument(
        "--end_year",
        type=int,
        default=config.END_YEAR,
        help=f"End year for processing (default: {config.END_YEAR})"
    )
    
    # Job control
    parser.add_argument(
        "--max_concurrent",
        type=int,
        default=20,
        help="Maximum number of concurrent jobs (default: 20)"
    )
    parser.add_argument(
        "--optimization_mode",
        type=str,
        choices=["balanced", "io_optimized", "compute_optimized", "fully_optimized"],
        default="io_optimized",
        help="Optimization mode for worker configuration (default: io_optimized)"
    )
    parser.add_argument(
        "--use_job_arrays",
        action="store_true",
        help="Use SLURM job arrays for years (one array per variable)"
    )
    parser.add_argument(
        "--no_submit",
        action="store_true",
        help="Generate job scripts but don't submit them"
    )
    parser.add_argument(
        "--output_dir",
        type=Path,
        default=config.OUTPUT_DIR,
        help=f"Output directory (default: {config.OUTPUT_DIR})"
    )
    parser.add_argument(
        "--wait_time",
        type=int,
        default=2,
        help="Wait time in seconds between job submissions (default: 2)"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    return args


def get_variables_to_process(args: argparse.Namespace) -> List[str]:
    """Get the list of variables to process.
    
    Args:
        args: Parsed command line arguments
    
    Returns:
        List of variable names to process
    """
    if args.variable:
        variables = [args.variable]
    elif args.variables:
        variables = [v.strip() for v in args.variables.split(",") if v.strip()]
    elif args.all_variables:
        variables = list_all_variables()
    else:
        raise ValueError("No variables specified")
    
    # Validate variables
    invalid_vars = [v for v in variables if v not in era5_datavar_lut]
    if invalid_vars:
        logging.error(f"Invalid variables: {', '.join(invalid_vars)}")
        logging.error(f"Available variables: {', '.join(list(era5_datavar_lut.keys())[:10])}...")
        sys.exit(1)
    
    return variables


def submit_individual_jobs(
    variables: List[str],
    start_year: int,
    end_year: int,
    max_concurrent: int,
    no_submit: bool,
    wait_time: int,
    optimization_mode: str = "io_optimized"
) -> Dict[str, List[str]]:
    """Submit individual jobs for each variable and year.
    
    Args:
        variables: List of variables to process
        start_year: Start year for processing
        end_year: End year for processing
        max_concurrent: Maximum number of concurrent jobs
        no_submit: Whether to skip job submission
        wait_time: Wait time between job submissions
        optimization_mode: Optimization mode for worker configuration
    
    Returns:
        Dictionary mapping variables to lists of job IDs
    """
    job_ids = {}
    
    # Count total jobs
    total_jobs = len(variables) * (end_year - start_year + 1)
    logging.info(f"Preparing to submit {total_jobs} jobs for {len(variables)} variables and {end_year - start_year + 1} years")
    
    # Process each variable and year
    for variable in variables:
        job_ids[variable] = []
        for year in range(start_year, end_year + 1):
            # Submit the job
            if no_submit:
                logging.info(f"Would submit job for variable {variable}, year {year}")
                job_id = f"DUMMY_{variable}_{year}"
            else:
                # Check if we need to wait for jobs to complete
                while True:
                    # Get number of running and pending jobs
                    try:
                        result = subprocess.run(
                            ["squeue", "--noheader", "--format=%i", "--user", os.environ["USER"]],
                            capture_output=True,
                            text=True,
                            check=True
                        )
                        current_jobs = len(result.stdout.strip().split("\n")) if result.stdout.strip() else 0
                        
                        if current_jobs < max_concurrent:
                            break
                        
                        logging.info(f"Currently {current_jobs} jobs in queue, waiting for some to complete...")
                        time.sleep(30)  # Wait 30 seconds before checking again
                    except subprocess.CalledProcessError:
                        logging.warning("Failed to check current job count, proceeding with submission")
                        break
                
                # Submit the job
                cmd = ["sbatch", "process_era5_variable.sbatch", str(year), variable, optimization_mode]
                logging.info(f"Submitting: {' '.join(cmd)}")
                
                try:
                    result = subprocess.run(
                        cmd,
                        capture_output=True,
                        text=True,
                        check=True
                    )
                    job_id = result.stdout.strip().split()[-1]
                    logging.info(f"Submitted job {job_id} for variable {variable}, year {year}")
                    time.sleep(wait_time)  # Avoid overwhelming the scheduler
                except subprocess.CalledProcessError as e:
                    logging.error(f"Failed to submit job for {variable}, year {year}: {e}")
                    logging.error(f"stdout: {e.stdout}")
                    logging.error(f"stderr: {e.stderr}")
                    job_id = None
            
            if job_id:
                job_ids[variable].append(job_id)
    
    return job_ids


def generate_job_array(
    variable: str,
    start_year: int,
    end_year: int,
    output_dir: Path,
    optimization_mode: str = "io_optimized"
) -> Tuple[Path, str]:
    """Generate a job array script for a variable.
    
    Args:
        variable: Variable to process
        start_year: Start year for processing
        end_year: End year for processing
        output_dir: Output directory for results
        optimization_mode: Optimization mode for worker configuration
    
    Returns:
        Tuple of (script path, year list string)
    """
    # Create scripts directory if it doesn't exist
    scripts_dir = Path("job_scripts")
    scripts_dir.mkdir(exist_ok=True)
    
    # Create year mapping file for array jobs
    year_map_file = scripts_dir / f"{variable}_{start_year}_{end_year}_year_map.txt"
    
    # Write year mapping
    with open(year_map_file, "w") as f:
        for i, year in enumerate(range(start_year, end_year + 1), 1):
            f.write(f"{i}\t{year}\n")
    
    # Get array size
    array_size = end_year - start_year + 1
    
    # Create the script content
    script_content = f"""#!/bin/bash
#SBATCH --job-name=era5_{variable}
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=24
#SBATCH --mem=96G
#SBATCH --time=12:00:00
#SBATCH --partition=t2small
#SBATCH --output=era5_{variable}_%A_%a.out
#SBATCH --array=1-{array_size}%5

# Get year from year map file
YEAR_FILE={year_map_file}
YEAR=$(awk -v idx=$SLURM_ARRAY_TASK_ID '{{if($1==idx) print $2}}' $YEAR_FILE)

if [ -z "$YEAR" ]; then
    echo "Error: Could not determine year for array task $SLURM_ARRAY_TASK_ID"
    exit 1
fi

echo "Starting at $(date)"
echo "Running on $(hostname)"
echo "Processing variable {variable} for year $YEAR"

# Activate conda environment
source $HOME/miniconda3/etc/profile.d/conda.sh
conda activate snap-geo

# Install psutil if it's not already available
if ! python -c "import psutil" &> /dev/null; then
    echo "Installing psutil for memory monitoring"
    pip install psutil
fi

# Set OpenMP threads
export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK

# Set HDF5 cache settings to improve netCDF performance
export HDF5_USE_FILE_LOCKING=FALSE
export HDF5_CACHE_SIZE=1073741824  # 1GB cache

# Run the Python script with optimized settings
python process_single_variable.py \\
    --year $YEAR \\
    --variable {variable} \\
    --cores $SLURM_CPUS_PER_TASK \\
    --memory_limit "85GB" \\
    --optimization_mode "{optimization_mode}" \\
    --monitor_memory \\
    --monitor_interval 60 \\
    --recurse_limit 100

# Check the exit status
if [ $? -eq 0 ]; then
    echo "Processing completed successfully"
else
    EXIT_CODE=$?
    echo "Processing failed with exit code $EXIT_CODE"
    exit $EXIT_CODE
fi

echo "Finished at $(date)"
"""
    
    # Write the script to file
    script_path = scripts_dir.joinpath(f"process_{variable}_{start_year}_{end_year}.sbatch")
    with open(script_path, "w") as f:
        f.write(script_content)
    
    return script_path, script_content


def submit_job_arrays(
    variables: List[str],
    start_year: int,
    end_year: int,
    output_dir: Path,
    no_submit: bool,
    wait_time: int,
    optimization_mode: str = "io_optimized"
) -> Dict[str, str]:
    """Submit job arrays for each variable.
    
    Args:
        variables: List of variables to process
        start_year: Start year for processing
        end_year: End year for processing
        output_dir: Output directory for results
        no_submit: Whether to skip job submission
        wait_time: Wait time between job submissions
        optimization_mode: Optimization mode for worker configuration
    
    Returns:
        Dictionary mapping variables to job array IDs
    """
    job_ids = {}
    
    for variable in variables:
        # Generate job array script
        script_path, _ = generate_job_array(variable, start_year, end_year, output_dir, optimization_mode)
        
        # Submit the job
        if no_submit:
            logging.info(f"Would submit job array for variable {variable}, years {start_year}-{end_year}")
            job_id = f"DUMMY_{variable}_array"
        else:
            cmd = ["sbatch", str(script_path)]
            logging.info(f"Submitting: {' '.join(cmd)}")
            
            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    check=True
                )
                job_id = result.stdout.strip().split()[-1]
                logging.info(f"Submitted job array {job_id} for variable {variable}, years {start_year}-{end_year}")
                time.sleep(wait_time)  # Avoid overwhelming the scheduler
            except subprocess.CalledProcessError as e:
                logging.error(f"Failed to submit job array for {variable}: {e}")
                logging.error(f"stdout: {e.stdout}")
                logging.error(f"stderr: {e.stderr}")
                job_id = None
        
        if job_id:
            job_ids[variable] = job_id
    
    return job_ids


def main() -> None:
    """Main function for job submission."""
    # Parse command line arguments
    args = parse_args()
    setup_logging(args.verbose)
    
    # Get variables to process
    variables = get_variables_to_process(args)
    
    # Submit jobs
    if args.use_job_arrays:
        # Submit job arrays (one array per variable)
        job_ids = submit_job_arrays(
            variables,
            args.start_year,
            args.end_year,
            args.output_dir,
            args.no_submit,
            args.wait_time,
            args.optimization_mode
        )
    else:
        # Submit individual jobs
        job_ids = submit_individual_jobs(
            variables,
            args.start_year,
            args.end_year,
            args.max_concurrent,
            args.no_submit,
            args.wait_time,
            args.optimization_mode
        )
    
    # Print summary
    if args.no_submit:
        logging.info(f"Generated job scripts for {len(variables)} variables and {args.end_year - args.start_year + 1} years")
    else:
        logging.info(f"Submitted jobs for {len(variables)} variables and {args.end_year - args.start_year + 1} years")
        
        total_jobs = sum(len(jobs) if isinstance(jobs, list) else 1 for jobs in job_ids.values())
        logging.info(f"Total jobs submitted: {total_jobs}")


if __name__ == "__main__":
    main() 