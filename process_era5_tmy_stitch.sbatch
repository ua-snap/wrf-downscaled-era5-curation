#!/bin/bash
#SBATCH --job-name=tmy_stitch
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=24
#SBATCH --mem=96G
#SBATCH --time=01:00:00
#SBATCH --partition=t2small
#SBATCH --output=logs/era5_tmy/stitch_%j.out

# SLURM Job for Stitching TMY Monthly Files
#
# Usage:
#   sbatch --dependency=afterok:JOBID process_era5_tmy_stitch.sbatch <start_year> <end_year>
#
# Example:
#   # After job array 12345 completes
#   sbatch --dependency=afterok:12345 process_era5_tmy_stitch.sbatch 2010 2020

echo "=========================================="
echo "TMY Stitching Job"
echo "=========================================="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $(hostname)"
echo "Started at: $(date)"
echo "=========================================="

# Activate conda environment
source $HOME/miniconda3/etc/profile.d/conda.sh
conda activate snap-geo

# Set OpenMP threads
export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK

# Get command line arguments
START_YEAR=${1:-2010}
END_YEAR=${2:-2020}
SPATIAL_SUBSET_FLAG=$3  # "subset" or empty

# Create log directory
mkdir -p logs/era5_tmy

# Set environment variables
export ERA5_INPUT_DIR=/beegfs/CMIP6/wrf_era5/04km
export ERA5_OUTPUT_DIR=/beegfs/CMIP6/cparr4/era5_tmy

echo "Stitching TMY for years $START_YEAR to $END_YEAR"

# Build command
CMD="python stitch_tmy.py --start_year $START_YEAR --end_year $END_YEAR"

# Add spatial subset flag if provided
if [ "$SPATIAL_SUBSET_FLAG" = "subset" ]; then
    echo "Processing spatial subset files"
    CMD="$CMD --spatial_subset"
fi

# Run the Python script
echo "Executing: $CMD"
$CMD

# Check exit status
if [ $? -eq 0 ]; then
    echo "TMY stitching completed successfully at $(date)"
    echo "=========================================="
    echo "FINAL TMY FILE READY!"
    echo "=========================================="
else
    EXIT_CODE=$?
    echo "TMY stitching failed with exit code $EXIT_CODE at $(date)"
    if [ $EXIT_CODE -eq 137 ] || [ $EXIT_CODE -eq 9 ]; then
        echo "This appears to be a memory-related failure."
    fi
    exit $EXIT_CODE
fi

echo "=========================================="
echo "Finished at: $(date)"
echo "=========================================="
