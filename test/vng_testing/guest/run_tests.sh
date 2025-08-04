#!/bin/bash
RESULTS_DIR="./results/test_failures.log"
KERNEL_VERSION="$1"

mount -t tmpfs -o size=512M tmpfs /cvmfs
mount /dev/vda /mnt/cvmfs_cache

echo "Running tests from directory: ./tests"
echo "Sourcing test_functions"
source ./test_functions

# Loop through each test and execute it
for test in ./tests/*; do
    if [ -d "$test" ] && [ -f "$test/main" ]; then
        test_name=$(basename "$test")
        timestamp=$(date +"%Y-%m-%d %H:%M:%S")
        echo "[$timestamp] Running test: $test_name on kernel $KERNEL_VERSION"

        # Run the test script
        source "$test/main"
        cvmfs_run_test
        exit_code=$?

        timestamp=$(date +"%Y-%m-%d %H:%M:%S")
        # Check if the test passed
        if [ $exit_code -eq 0 ]; then
            echo "Test $test_name passed on $KERNEL_VERSION."
        else
            echo "Test $test_name failed with exit code $exit_code on $KERNEL_VERSION."
            echo "[$timestamp] [$KERNEL_VERSION] $test_name : $exit_code" >> "$RESULTS_DIR"
        fi
    fi
done
