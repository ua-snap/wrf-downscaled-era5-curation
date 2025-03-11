#!/bin/bash

# Development environment settings for ERA5 processing
# Usage: source dev_env.sh

# Set minimal data variables for processing
export ERA5_DATA_VARS="t2min, albedo_mean"

# Set a small year for processing
export ERA5_START_YEAR="2019"
export ERA5_END_YEAR="2021"

# Set development paths (you may want to adjust these)
export ERA5_INPUT_DIR="/beegfs/CMIP6/wrf_era5/04km"
export ERA5_OUTPUT_DIR="./dev_output"

# Enable overwriting for development
export ERA5_OVERWRITE="True"


echo "Development environment variables set:"
echo "  Processing variable: $ERA5_DATA_VARS"
echo "  Year range: $ERA5_START_YEAR-$ERA5_END_YEAR"
echo "  Output directory: $ERA5_OUTPUT_DIR"