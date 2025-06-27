#!/bin/bash
#SBATCH --job-name=era5_process
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=24
#SBATCH --mem=96G
#SBATCH --time=00:07:00
#SBATCH --partition=t2small
#SBATCH --output=logs/era5_process/%x_%j.out

# Usage:
# sbatch process_era5_variable.sbatch <year> <variable> [overwrite]
# Example:
# sbatch process_era5_variable.sbatch 1980 t2_mean

echo "Starting at $(date)"
echo "Running on $(hostname)"

# Activate conda environment
source $HOME/miniconda3/etc/profile.d/conda.sh
conda activate snap-geo


# Set OpenMP threads, not sure if this is useful
# Consider the status to be "superstition" at this point
export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK

# Get command line arguments
YEAR=$1
VARIABLE=$2
OVERWRITE=$3  # Optional, set to "overwrite" to force reprocessing

# Validate arguments
if [ -z "$YEAR" ] || [ -z "$VARIABLE" ]; then
    echo "Error: Missing arguments"
    echo "Usage: sbatch process_era5_variable.sbatch <year> <variable> [overwrite]"
    exit 1
fi

# Create log directory for this variable
mkdir -p logs/era5_process/${VARIABLE}

echo "Processing variable $VARIABLE for year $YEAR"

# Set overwrite environment variable if requested
if [ "$OVERWRITE" = "overwrite" ]; then
    echo "Forcing overwrite of existing files"
    OVERWRITE_FLAG="--overwrite"
else
    OVERWRITE_FLAG=""
fi

# Run the Python script
python process_single_variable.py \
    --year $YEAR \
    --variable $VARIABLE \
    $OVERWRITE_FLAG
# Check the exit status
if [ $? -eq 0 ]; then
    echo "Processing completed successfully"
else
    EXIT_CODE=$?
    echo "Processing failed with exit code $EXIT_CODE"
    # If this is a memory error, it might be helpful to run with more memory
    if [ $EXIT_CODE -eq 137 ] || [ $EXIT_CODE -eq 9 ]; then
        echo "This appears to be a memory-related failure. Consider increasing the memory allocation."
    fi
    exit $EXIT_CODE
fi

echo "Finished at $(date)" 
