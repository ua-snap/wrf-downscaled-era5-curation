"""Script to build a slurm file that runs the worker script resample_and_regrid_era5.py.


example usage:
    python /beegfs/CMIP6/$USER/repos/cmip6-utils/downscaling/run_resample_and_regrid_era5.py
        --conda_env_name snap-geo
        --output_directory /beegfs/CMIP6/$USER/daily_era5_4km_3338/netcdf
        --slurm_directory /beegfs/CMIP6/$USER/daily_era5_4km_3338/netcdf
"""

import argparse
import logging
import subprocess
from pathlib import Path

from config import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)


def submit_sbatch(sbatch_fp):
    """Submit a script to slurm via sbatch.

    Args:
        sbatch_fp : pathlib.PosixPath
        path to .slurm script to submit

    Returns
    -------
    str
        job id for submitted job
    """
    out = subprocess.check_output(["sbatch", str(sbatch_fp)])
    job_id = out.decode().replace("\n", "").split(" ")[-1]

    return job_id


def write_config_file(config_path, start_year, end_year):
    """Write a config file for the resampling script.

    Parameters
    ----------
    config_path : pathlib.PosixPath
        path to write the config file
    start_year : str
        start year for processing
    end_year : str
        end year for processing
    """
    with open(config_path, "w") as f:
        f.write("array_id\tyear\n")
        for array_id, year in enumerate(
            range(int(start_year), int(end_year) + 1), start=1
        ):
            f.write(f"{array_id}\t{year}\n")


def get_array_range(start_year, end_year):
    """Get the range of years to process for the SLURM array.

    Parameters
    ----------
    start_year : str
        start year for processing
    end_year : str
        end year for processing

    Returns
    -------
    str
        string to use in the SLURM array
    """
    return f"1-{int(end_year) - int(start_year) + 1}"


def parse_args():
    """Parse some command line arguments.

    Returns
    -------
    conda_env_name : str
        Name of conda environment to activate
    output_directory : str
        Path to directory where resampled and reprojected ERA5 data will be written
    slurm_directory : str
        Path to directory for writing slurm files
    """

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--conda_env_name",
        type=str,
        help="Name of conda environment to activate",
        required=True,
    )
    parser.add_argument(
        "--output_directory",
        type=str,
        help="Path to directory where resampled and reprojected ERA5 data will be stored",
        required=True,
    )
    parser.add_argument(
        "--slurm_directory",
        type=str,
        help="Path to directory for writing slurm files",
        required=True,
    )

    args = parser.parse_args()

    return (
        args.conda_env_name,
        Path(args.output_directory),
        Path(args.slurm_directory),
    )


if __name__ == "__main__":

    (
        conda_env_name,
        output_directory,
        slurm_directory,
    ) = parse_args()

    wrf_era5_directory = config.INPUT_DIR

    output_directory.mkdir(exist_ok=True)
    slurm_directory.mkdir(exist_ok=True)
    process_era5_sbatch_file = slurm_directory.joinpath(
        "resample_and_regrid_era5.slurm"
    )
    process_era5_sbatch_out_file = str(process_era5_sbatch_file).replace(
        ".slurm", "_%j.out"
    )
    config_file = slurm_directory.joinpath("slurm_array_config.txt")

    start_year = config.START_YEAR
    end_year = config.END_YEAR
    write_config_file(config_file, start_year, end_year)
    array_range = get_array_range(start_year, end_year)

    sbatch_text = (
        "#!/bin/sh\n"
        f"#SBATCH --array={array_range}%10\n"  # don't run more than 10 tasks
        f"#SBATCH --job-name=era5_{start_year}_{end_year}\n"
        "#SBATCH --nodes=1\n"
        # f"#SBATCH --cpus-per-task=24\n"
        f"#SBATCH -p t2small\n"
        f"#SBATCH --time=08:00:00\n"
        f"#SBATCH --output {process_era5_sbatch_out_file}\n"
        "echo Start slurm && date\n"
        # this should work to initialize conda without a conda init script
        'eval "$($HOME/miniconda3/bin/conda shell.bash hook)"\n'
        f"conda activate {conda_env_name}\n"
        f"config={config_file}\n"
        # Extract the year to process for the current $SLURM_ARRAY_TASK_ID
        "year=$(awk -v array_id=$SLURM_ARRAY_TASK_ID '$1==array_id {print $2}' $config)\n"
        f"python resample_and_regrid_era5.py --era5_dir {wrf_era5_directory} "
        f"--output_dir {output_directory} --year $year --geo_file {config.GEO_EM_FILE} "
    )
    print(config.OVERWRITE)
    print(type(config.OVERWRITE))
    if not config.OVERWRITE:
        sbatch_text += "--no_clobber"

    # save the sbatch text as a new slurm file in the repo directory
    with open(process_era5_sbatch_file, "w") as f:
        f.write(sbatch_text)

    logging.info(
        f"Submitting {process_era5_sbatch_file} to slurm (contents:)\n{sbatch_text}"
    )
    job_id = submit_sbatch(process_era5_sbatch_file)

    # print the job_id to stdout
    # there is no way to set an env var for the parent shell, so the only ways to
    # directly pass job ids are through stdout or a temp file
    print(job_id)
