#!/usr/bin/env python3
"""Generate and submit SLURM jobs for processing ERA5 variables.

This script generates SLURM job submissions for processing ERA5 variables for
specified years. It provides options for controlling the number of concurrent
jobs.

Example usage:
    # Process t2_mean for years 1980-1985
    python submit_era5_jobs.py --variable t2_mean --start_year 1980 --end_year 1985
    
    # Process multiple variables for a range of years
    python submit_era5_jobs.py --variables t2_mean,t2_min,t2_max --start_year 1990 --end_year 2000
    
    # Process all variables for a specific year
    python submit_era5_jobs.py --all_variables --start_year 2000 --end_year 2000
"""

import argparse
import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Dict

from era5_variables import era5_datavar_lut, list_all_variables
from config import config
from utils.logging import get_logger, create_log_directory, setup_variable_logging

# Get a named logger for this module
logger = get_logger(__name__)


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
        "--output_dir",
        type=Path,
        default=config.OUTPUT_DIR,
        help=f"Output directory (default: {config.OUTPUT_DIR})"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing output files"
    )
    
    return parser.parse_args()


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


def create_job_log_directory(variable: str) -> Path:
    """Create a log directory for a variable's job outputs.
    
    Creates a directory structure: logs/era5_process/{variable}
    
    Args:
        variable: Variable name
        
    Returns:
        Path to the log directory
    """
    # Create the log directory structure using the utility function
    log_dir = create_log_directory(Path.cwd(), f"era5_process/{variable}")
    
    logger.info(f"Created log directory: {log_dir}")
    return log_dir


def submit_individual_jobs(
    variables: List[str],
    start_year: int,
    end_year: int,
    max_concurrent: int,
    overwrite: bool = False
) -> Dict[str, List[str]]:
    """Submit individual jobs for each variable and year.
    
    Args:
        variables: List of variables to process
        start_year: Start year for processing
        end_year: End year for processing
        max_concurrent: Maximum number of concurrent jobs
        overwrite: Whether to overwrite existing output files
    
    Returns:
        Dictionary mapping variables to lists of job IDs
    """
    job_ids = {}
    wait_time = 2  # hardcoded value, used to not overwhelm the scheduler
    
    # Count total jobs
    total_jobs = len(variables) * (end_year - start_year + 1)
    logger.info(f"Preparing to submit {total_jobs} jobs for {len(variables)} variables and {end_year - start_year + 1} years")
    
    # Process each variable and year
    for variable in variables:
        job_ids[variable] = []
        
        # Create log directory for this variable
        log_dir = create_job_log_directory(variable)
        
        for year in range(start_year, end_year + 1):
            # Submit job but check if we need to wait for jobs to complete
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
                    
                    logger.info(f"Currently {current_jobs} jobs in queue, waiting for some to complete...")
                    time.sleep(30)  # Wait 30 seconds before checking again
                except subprocess.CalledProcessError:
                    logger.warning("Failed to check current job count, proceeding with submission")
                    break
            
            # Submit the job with log directory specified
            log_file = log_dir.joinpath(f"era5_{variable}_{year}.out")
            cmd = ["sbatch", "--output", str(log_file), "process_era5_variable.sbatch", str(year), variable,]
            if overwrite:
                cmd.append("overwrite")
            logger.info(f"Submitting: {' '.join(cmd)}")
            
            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    check=True
                )
                job_id = result.stdout.strip().split()[-1]
                logger.info(f"Submitted job {job_id} for variable {variable}, year {year}")
                time.sleep(wait_time)  # Avoid overwhelming the scheduler
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to submit job for {variable}, year {year}: {e}")
                logger.error(f"stdout: {e.stdout}")
                logger.error(f"stderr: {e.stderr}")
                job_id = None
        
        if job_id:
            job_ids[variable].append(job_id)
    
    return job_ids


def main() -> None:
    """Main function for job submission."""
    # Configure logging for the job submission process
    setup_variable_logging(
        variable="submit_era5_jobs",
        base_dir=Path.cwd(),
        verbose=False
    )
    
    args = parse_args()
    
    # Process variables
    variables = get_variables_to_process(args)
    
    # Submit jobs
    job_ids = submit_individual_jobs(
        variables=variables,
        start_year=args.start_year,
        end_year=args.end_year,
        max_concurrent=args.max_concurrent,
        overwrite=args.overwrite
    )
    
    logger.info(f"Job submission complete. Submitted jobs for {len(job_ids)} variables.")
    for variable, ids in job_ids.items():
        if ids:
            logger.info(f"  - {variable}: {len(ids)} jobs submitted")
    
    logger.info("Done.")


if __name__ == "__main__":
    main() 