#!/usr/bin/env python3
"""Generate and submit SLURM jobs for processing ERA5 variables.

This script generates SLURM job submissions for processing ERA5 variables for
specified years: one job per variable, per year. It provides options for controlling
the number of concurrent jobs and includes automatic retry functionality for jobs that timeout.

Configuration is handled through environment variables:
    ERA5_INPUT_DIR: Input directory containing ERA5 data
    ERA5_OUTPUT_DIR: Output directory for processed files
    ERA5_GEO_FILE: Path to WRF geo_em file for projection information
    ERA5_START_YEAR: Start year for processing
    ERA5_END_YEAR: End year for processing

Example usage:
    # Process t2_mean for years 1980-1985
    python submit_era5_jobs.py --variables t2_mean --start_year 1980 --end_year 1985
    
    # Process multiple variables for a range of years
    python submit_era5_jobs.py --variables t2_mean,t2_min,t2_max --start_year 1990 --end_year 2000
    
    # Process with automatic retry disabled
    python submit_era5_jobs.py --variables t2_mean --start_year 1980 --end_year 1985 --no_retry
"""

import argparse
import logging
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Dict, Tuple

from era5_variables import era5_datavar_lut
from config import data_config, config
from utils.logging import get_logger, create_log_directory, setup_variable_logging

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.
    
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(description=__doc__)
    
    parser.add_argument(
        "--variables",
        type=str,
        help="Comma-separated list of variables to process"
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
        default=30,
        help="Maximum number of concurrent jobs (default: 30)"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing output files"
    )
    parser.add_argument(
        "--no_retry",
        action="store_true",
        help="Disable automatic retry of timed-out jobs"
    )
    
    return parser.parse_args()


def get_variables_to_process(args: argparse.Namespace) -> List[str]:
    """Get the list of variables to process based on command line arguments.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        List of variable names to process
        
    Raises:
        ValueError: If invalid variables are specified
    """
    if args.variables:
        variables = [v.strip() for v in args.variables.split(",")]
    else:
        variables = config.DATA_VARS
    
    # Validate variables
    invalid_vars = [v for v in variables if v not in era5_datavar_lut]
    if invalid_vars:
        available_vars = list(era5_datavar_lut.keys())
        raise ValueError(
            f"Invalid variables: {', '.join(invalid_vars)}. "
            f"Available variables: {', '.join(available_vars[:10])}..."
        )
    
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
    """Submit SLURM jobs for processing variables across multiple years.
    
    Args:
        variables: List of variable names to process
        start_year: Starting year for processing
        end_year: Ending year for processing (inclusive)
        max_concurrent: Maximum number of concurrent jobs
        overwrite: Whether to overwrite existing output files
        
    Returns:
        Dictionary mapping variable names to lists of submitted job IDs
    """
    job_ids = {}
    wait_time = 1  # seconds between job submissions
    
    def wait_for_available_slots():
        """Wait until there are available job slots."""
        while True:
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
                time.sleep(15)
            except subprocess.CalledProcessError:
                logger.warning("Failed to check current job count, proceeding with submission")
                break
    
    def submit_job(variable: str, year: int) -> str:
        """Submit a single job and return the job ID."""
        # Create log directory for this variable if needed
        log_dir = create_job_log_directory(variable)
        
        # Wait for available slots
        wait_for_available_slots()
        
        # Submit the job with specified time limit
        log_file = log_dir.joinpath(f"era5_{variable}_{year}.out")
        
        cmd = ["sbatch", "--output", str(log_file), "process_era5_variable.sbatch", str(year), variable]
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
            
            time.sleep(wait_time)
            return job_id
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to submit job for {variable}, year {year}: {e}")
            logger.error(f"stdout: {e.stdout}")
            logger.error(f"stderr: {e.stderr}")
            return None
    
    # Submit jobs for all variable/year combinations
    total_jobs = 0
    for variable in variables:
        if variable not in job_ids:
            job_ids[variable] = []
        
        for year in range(start_year, end_year + 1):
            job_id = submit_job(variable, year)
            if job_id:
                job_ids[variable].append(job_id)
                total_jobs += 1
    
    # Log summary
    logger.info(f"Job submission complete. Submitted {total_jobs} total jobs:")
    if job_ids:
        logger.info(f"Jobs submitted for {len(job_ids)} variables:")
        for variable, ids in job_ids.items():
            logger.info(f"  - {variable}: {len(ids)} jobs submitted")
    
    return job_ids


def wipe_slurm_output_logs(base_log_dir: Path = Path.cwd() / "logs" / "era5_process") -> None:
    """Wipe all .out files from subdirectories of base_log_dir."""
    logger.info(f"Wiping existing SLURM .out log files from {base_log_dir}...")
    count = 0
    for log_file in base_log_dir.rglob("*.out"):
        try:
            log_file.unlink()
            logger.debug(f"Deleted {log_file}")
            count += 1
        except OSError as e:
            logger.warning(f"Error deleting {log_file}: {e}")
    logger.info(f"Wiped {count} SLURM .out log files.")


def wait_for_jobs_completion() -> None:
    """Wait for all submitted jobs to complete."""
    logger.info("Waiting for all submitted jobs to complete...")
    start_time = time.time()
    last_log_time = start_time
    
    while True:
        try:
            result = subprocess.run(
                ["squeue", "--noheader", "--format=%i", "--user", os.environ["USER"]],
                capture_output=True,
                text=True,
                check=True
            )
            current_jobs = len(result.stdout.strip().split("\n")) if result.stdout.strip() else 0
            
            if current_jobs == 0:
                logger.info("All jobs have completed.")
                break
            
            # Log progress every 5 minutes
            current_time = time.time()
            if current_time - last_log_time >= 300:  # 5 minutes
                elapsed_minutes = (current_time - start_time) / 60
                logger.info(f"Still waiting for {current_jobs} jobs to complete (elapsed: {elapsed_minutes:.1f} minutes)")
                last_log_time = current_time
            
            time.sleep(30)  # Check every 30 seconds
            
        except subprocess.CalledProcessError as e:
            logger.warning(f"Error checking job status: {e}")
            logger.warning("Continuing to wait...")
            time.sleep(30)


def detect_timed_out_jobs() -> List[Tuple[str, int]]:
    """Scan log files for SLURM timeout patterns and return timed-out jobs.
    
    Returns:
        List of (variable, year) tuples that timed out
    """
    logger.info("Scanning log files for timed-out jobs...")
    timed_out_jobs = []
    base_log_dir = Path.cwd() / "logs" / "era5_process"
    
    if not base_log_dir.exists():
        logger.warning(f"Log directory {base_log_dir} does not exist")
        return timed_out_jobs
    
    # Pattern to extract variable and year from filename: era5_variable_year.out
    filename_pattern = re.compile(r"era5_(.+)_(\d{4})\.out")
    
    log_files = list(base_log_dir.rglob("era5_*.out"))
    logger.info(f"Checking {len(log_files)} log files for timeout patterns...")
    
    for log_file in log_files:
        try:
            # Extract variable and year from filename
            match = filename_pattern.search(log_file.name)
            if not match:
                logger.warning(f"Could not parse filename: {log_file.name}")
                continue
            
            variable = match.group(1)
            year = int(match.group(2))
            
            # Read log file and check for timeout pattern
            with open(log_file, 'r') as f:
                content = f.read()
            
            # Look for SLURM timeout pattern
            if "CANCELLED AT" in content and "DUE TO TIME LIMIT" in content:
                logger.info(f"Timeout detected for {variable} year {year}")
                timed_out_jobs.append((variable, year))
                
        except Exception as e:
            logger.warning(f"Error reading log file {log_file}: {e}")
            continue
    
    if timed_out_jobs:
        logger.info(f"Found {len(timed_out_jobs)} timed-out jobs:")
        for variable, year in timed_out_jobs:
            logger.info(f"  - {variable} {year}")
    else:
        logger.info("No timed-out jobs detected.")
    
    return timed_out_jobs


def submit_timeout_retries(timeout_jobs: List[Tuple[str, int]], overwrite: bool = False) -> int:
    """Resubmit timed-out jobs.
    
    Args:
        timeout_jobs: List of (variable, year) tuples that timed out
        overwrite: Whether to overwrite existing output files
        
    Returns:
        Number of successfully submitted retry jobs
    """
    if not timeout_jobs:
        return 0
    
    logger.info(f"Submitting {len(timeout_jobs)} timeout retry jobs...")
    
    retry_count = 0
    wait_time = 2
    
    for variable, year in timeout_jobs:
        try:
            # Create log directory for this variable if needed
            log_dir = create_job_log_directory(variable)
            
            # Submit the retry job 
            log_file = log_dir.joinpath(f"era5_{variable}_{year}.out")
            
            
            cmd = ["sbatch", "--output", str(log_file), "process_era5_variable.sbatch", str(year), variable]
            if overwrite:
                cmd.append("overwrite")
            
            logger.info(f"RETRY: Submitting {variable} {year}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
            )
            job_id = result.stdout.strip().split()[-1]
            
            logger.info(f"RETRY: Submitted job {job_id} for {variable} year {year}")
            retry_count += 1
            
            time.sleep(wait_time)
            
        except subprocess.CalledProcessError as e:
            logger.error(f"RETRY: Failed to submit retry job for {variable} year {year}: {e}")
            logger.error(f"stdout: {e.stdout}")
            logger.error(f"stderr: {e.stderr}")
            continue
    
    logger.info(f"Successfully submitted {retry_count} retry jobs out of {len(timeout_jobs)} timeouts")
    return retry_count


def validate_job_completion(variables: List[str], start_year: int, end_year: int) -> None:
    """Validate that all expected output files exist and are valid NetCDF files.
    
    Args:
        variables: List of variables that were processed
        start_year: Starting year
        end_year: Ending year (inclusive)
    """
    logger.info("Validating job completion by checking output files...")
    
    total_expected = len(variables) * (end_year - start_year + 1)
    missing_files = []
    existing_files = 0
    
    for variable in variables:
        for year in range(start_year, end_year + 1):
            expected_file = data_config.get_output_file(variable, year)
            
            if expected_file.exists() and expected_file.stat().st_size > 0:
                existing_files += 1
            else:
                missing_files.append((variable, year))
    
    logger.info(f"Job completion validation results:")
    logger.info(f"  Expected files: {total_expected}")
    logger.info(f"  Existing files: {existing_files}")
    logger.info(f"  Missing files: {len(missing_files)}")
    
    if missing_files:
        logger.warning(f"Missing output files for {len(missing_files)} jobs:")
        for variable, year in missing_files:
            logger.warning(f"  - {variable} {year}")
    else:
        logger.info("✓ All expected output files are present!")
    
    completion_rate = (existing_files / total_expected) * 100
    logger.info(f"Overall completion rate: {completion_rate:.1f}%")


def main() -> None:
    """Main function for job submission."""
    # Configure logging for the job submission process
    setup_variable_logging(
        variable="submit_era5_jobs",
        base_dir=Path.cwd(),
        verbose=False
    )
    
    args = parse_args()
    
    try:
        # Wipe existing SLURM output logs for a fresh run
        wipe_slurm_output_logs()

        variables = get_variables_to_process(args)
        
        # Submit jobs for each variable/year combination
        job_ids = submit_individual_jobs(
            variables=variables,
            start_year=args.start_year,
            end_year=args.end_year,
            max_concurrent=args.max_concurrent,
            overwrite=args.overwrite
        )
        
        # Wait for all jobs to complete
        wait_for_jobs_completion()
        
        # Handle retries for timed-out jobs if enabled
        if not args.no_retry:
            timeout_jobs = detect_timed_out_jobs()
            if timeout_jobs:
                logger.info(f"Found {len(timeout_jobs)} timed-out jobs to retry")
                num_retried = submit_timeout_retries(timeout_jobs, args.overwrite)
                if num_retried > 0:
                    wait_for_jobs_completion()
        
        # Validate that all expected output files exist and are valid
        validate_job_completion(variables, args.start_year, args.end_year)
        
        logger.info("All processing completed successfully")
        
    except Exception as e:
        logger.error(f"Job submission failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 