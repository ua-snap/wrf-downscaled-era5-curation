#!/bin/bash
TEST_VAR="t2_mean"
TEST_YEAR=1980
python process_single_variable.py --year $TEST_YEAR --variable $TEST_VAR --cores 4 --memory_limit "8GB" --monitor_memory --monitor_interval 5 --recurse_limit 100 --verbose
EXIT_CODE=$?
echo "Test exit code: $EXIT_CODE"
