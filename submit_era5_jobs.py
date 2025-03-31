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
from typing import List, Tuple, Dict

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
        default=config.OPTIMIZATION_MODE,
        help=f"Optimization mode for worker configuration (default: {config.OPTIMIZATION_MODE})"
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
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing output files"
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


def create_log_directory(variable: str) -> Path:
    """Create a log directory for a variable.
    
    Creates a directory structure: logs/era5_process/{variable}
    
    Args:
        variable: Variable name
        
    Returns:
        Path to the log directory
    """
    # Create the log directory structure
    log_dir = Path("logs/era5_process").joinpath(variable)
    log_dir.mkdir(parents=True, exist_ok=True)
    
    logging.info(f"Created log directory: {log_dir}")
    return log_dir


def submit_individual_jobs(
    variables: List[str],
    start_year: int,
    end_year: int,
    max_concurrent: int,
    no_submit: bool,
    wait_time: int,
    optimization_mode: str = config.OPTIMIZATION_MODE,
    overwrite: bool = False
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
        overwrite: Whether to overwrite existing output files
    
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
        
        # Create log directory for this variable
        log_dir = create_log_directory(variable)
        
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
                
                # Submit the job with log directory specified
                log_file = log_dir.joinpath(f"era5_{variable}_{year}.out")
                cmd = ["sbatch", "--output", str(log_file), "process_era5_variable.sbatch", str(year), variable, optimization_mode]
                if overwrite:
                    cmd.append("overwrite")
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


def main() -> None:
    """Main function for job submission."""
    # Parse command line arguments
    args = parse_args()
    setup_logging(args.verbose)
    
    # Get variables to process
    variables = get_variables_to_process(args)
    
    # Submit individual jobs
    job_ids = submit_individual_jobs(
        variables,
        args.start_year,
        args.end_year,
        args.max_concurrent,
        args.no_submit,
        args.wait_time,
        args.optimization_mode,
        args.overwrite
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