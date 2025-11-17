#!/usr/bin/env python3
"""Submit parallel TMY processing jobs.

This script submits a SLURM job array for processing 12 calendar months
in parallel, then submits a stitching job that runs after all months complete.

Example usage:
    python submit_tmy_parallel.py --start_year 2010 --end_year 2020
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

from utils.logging import get_logger, setup_variable_logging

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Submit parallel TMY processing jobs")
    parser.add_argument(
        "--start_year",
        type=int,
        default=2010,
        help="Start year for TMY calculation (default: 2010)",
    )
    parser.add_argument(
        "--end_year",
        type=int,
        default=2020,
        help="End year for TMY calculation (default: 2020)",
    )

    return parser.parse_args()


def submit_month_array(start_year: int, end_year: int) -> str:
    """Submit SLURM job array for processing 12 months.

    Args:
        start_year: Start year
        end_year: End year

    Returns:
        Job ID of the submitted array
    """
    logger.info("=" * 60)
    logger.info("SUBMITTING MONTHLY JOB ARRAY (12 jobs)")
    logger.info("=" * 60)

    # build the sbatch command
    # building SLURM scripts via Python...kinda our SOP
    cmd = [
        "sbatch",
        "process_era5_tmy_month_array.sbatch",
        str(start_year),
        str(end_year),
    ]
    logger.info(f"Command: {' '.join(cmd)}")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)

        # extract job ID from output, format: "Submitted batch job 12345"
        # retaining the ID should let us create job completion trigger for stitching
        job_id = result.stdout.strip().split()[-1]

        logger.info(f"  Submitted job array: {job_id}")
        logger.info("   Job array will process 12 calendar months in parallel")
        logger.info(f"  Monitor with: watch squeue -j {job_id}")
        logger.info(f"  View logs: tail -f logs/era5_tmy/month_{job_id}_*.out")

        return job_id

    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to submit job array: {e}")
        logger.error(f"stdout: {e.stdout}")
        logger.error(f"stderr: {e.stderr}")
        sys.exit(1)


def submit_stitch_job(array_job_id: str, start_year: int, end_year: int) -> str:
    """Submit stitching job with dependency on array completion.

    Args:
        array_job_id: Job ID of the monthly array
        start_year: Start year
        end_year: End year

    Returns:
        Job ID of the stitching job
    """
    logger.info("")
    logger.info("=" * 60)
    logger.info("SUBMITTING STITCHING JOB (runs after all months complete)")
    logger.info("=" * 60)

    # build command with dependency for month job array completion
    cmd = [
        "sbatch",
        f"--dependency=afterok:{array_job_id}",
        "process_era5_tmy_stitch.sbatch",
        str(start_year),
        str(end_year),
    ]
    logger.info(f"Command: {' '.join(cmd)}")
    logger.info(f"Dependency: Will run after job {array_job_id} completes successfully")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)

        # Extract job ID
        stitch_job_id = result.stdout.strip().split()[-1]

        logger.info(f"  Submitted TMY chronological stitching job: {stitch_job_id}")
        logger.info("   Job will begin once all 12 monthly jobs complete.")
        logger.info(f"  View log: tail -f logs/era5_tmy/stitch_{stitch_job_id}.out")

        return stitch_job_id

    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to submit stitching job: {e}")
        logger.error(f"stdout: {e.stdout}")
        logger.error(f"stderr: {e.stderr}")
        sys.exit(1)


def wait_for_completion(array_job_id: str, stitch_job_id: str) -> None:
    """Wait for both jobs to complete and report status.

    Args:
        array_job_id: Job ID of the monthly array
        stitch_job_id: Job ID of the stitching job
    """
    logger.info("")
    logger.info("=" * 60)
    logger.info("MONITORING JOB COMPLETION")
    logger.info("=" * 60)
    logger.info(f"Monthly array job: {array_job_id}")
    logger.info(f"Stitching job: {stitch_job_id}")
    logger.info("")
    logger.info("Waiting for jobs to complete...")
    logger.info("")

    start_time = time.time()
    last_log_time = start_time

    while True:
        try:
            # Check if either job is still running
            result = subprocess.run(
                [
                    "squeue",
                    "--noheader",
                    "--format=%i,%T",
                    "--jobs",
                    f"{array_job_id},{stitch_job_id}",
                ],
                capture_output=True,
                text=True,
                check=True,
            )

            running_jobs = (
                result.stdout.strip().split("\n") if result.stdout.strip() else []
            )

            if not running_jobs or running_jobs == [""]:
                # No jobs found - either completed or failed
                logger.info("")
                logger.info("✓ All jobs have finished!")
                logger.info(
                    f"Total time: {(time.time() - start_time) / 60:.1f} minutes"
                )
                break

            # Count job states
            job_states = {}
            for job_line in running_jobs:
                if "," in job_line:
                    job_id, state = job_line.split(",")
                    job_states[job_id] = state

            # Log progress every 5 minutes
            current_time = time.time()
            if current_time - last_log_time >= 300:
                elapsed_minutes = (current_time - start_time) / 60
                logger.info(f"Still running after {elapsed_minutes:.1f} minutes:")
                for job_id, state in job_states.items():
                    logger.info(f"  Job {job_id}: {state}")
                last_log_time = current_time

            time.sleep(30)  # Check every 30 seconds

        except subprocess.CalledProcessError as e:
            logger.warning(f"Error checking job status: {e}")
            time.sleep(30)
        except KeyboardInterrupt:
            logger.info("")
            logger.info("Monitoring interrupted by user.")
            logger.info(f"Jobs {array_job_id} and {stitch_job_id} are still running.")
            logger.info(f"Check status with: squeue -j {array_job_id},{stitch_job_id}")
            sys.exit(0)


def main() -> None:
    """Main function."""
    args = parse_args()

    # Set up logging
    setup_variable_logging(
        variable="submit_tmy_parallel", base_dir=Path.cwd(), console_only=True
    )

    try:
        # Submit monthly array
        array_job_id = submit_month_array(args.start_year, args.end_year)

        # Submit stitching job
        stitch_job_id = submit_stitch_job(array_job_id, args.start_year, args.end_year)

        logger.info("")
        logger.info("=" * 60)
        logger.info("SUBMISSION COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Monthly array: {array_job_id} (12 jobs)")
        logger.info(f"Stitching job: {stitch_job_id} (runs after array completes)")
        logger.info("")
        logger.info(f"  Monitor: watch squeue -j {array_job_id},{stitch_job_id}")
        logger.info(f"  Cancel: scancel {array_job_id},{stitch_job_id}")
        logger.info("  Logs: ls -lh logs/era5_tmy/")
        logger.info("=" * 60)

        wait_for_completion(array_job_id, stitch_job_id)

        sys.exit(0)

    except Exception as e:
        logger.error(f"Job submission failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
