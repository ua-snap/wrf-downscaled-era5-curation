#!/bin/bash
#SBATCH --job-name=tmy_month
#SBATCH --array=1-12
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=24
#SBATCH --mem=96G
#SBATCH --time=04:00:00
#SBATCH --partition=t2small
#SBATCH --output=logs/era5_tmy/month_%A_%a.out

# SLURM Job Array for Parallel TMY Month Processing
# Usage:
#   sbatch process_era5_tmy_month_array.sbatch <start_year> <end_year>
#
# Example:
#   sbatch process_era5_tmy_month_array.sbatch 2010 2020


echo "=========================================="
echo "TMY Month Processing - Job Array"
echo "=========================================="
echo "Job ID: $SLURM_JOB_ID"
echo "Array Task ID: $SLURM_ARRAY_TASK_ID"
echo "Month: $SLURM_ARRAY_TASK_ID"
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

# Create log directory
mkdir -p logs/era5_tmy

# Month is the array task ID, 1 through 12
MONTH=$SLURM_ARRAY_TASK_ID

echo "Processing month $MONTH for years $START_YEAR to $END_YEAR"

# Build command
CMD="python process_tmy_month.py --month $MONTH --start_year $START_YEAR --end_year $END_YEAR"

# Run the Python script
echo "Executing: $CMD"
$CMD

# Check exit status
if [ $? -eq 0 ]; then
    echo "Month $MONTH processing completed successfully at $(date)"
else
    EXIT_CODE=$?
    echo "Month $MONTH processing failed with exit code $EXIT_CODE at $(date)"
    exit $EXIT_CODE
fi

echo "=========================================="
echo "Finished at: $(date)"
echo "=========================================="
