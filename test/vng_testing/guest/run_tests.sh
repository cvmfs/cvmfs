#!/bin/bash
set -e

SCRIPT_DIR=$(realpath "$(dirname "$0")")
VNG_DIR=$(dirname "$SCRIPT_DIR")
RESULTS_DIR="$VNG_DIR/results/test_failures.log"
LOGFILE="$VNG_DIR/results/test_run.log"
KERNEL_VERSION="$1"
shift

exclusions="$CVMFS_TEST_EXCLUDE"
labels="$CVMFS_TEST_SUITES"
while [[ $# -gt 0 ]]; do
  case "$1" in
    -s)
      labels="$2"
      shift 2
      ;;
    -x)
      shift
      while [[ $# -ne 0 && x"$1" != x"--" ]]; do
        exclusions="$exclusions $1"
        shift
      done
      shift # get rid of '--'
      ;;
    *)
      break
      ;;
  esac
done

# Remaining args after "--" (if any) are test_list
test_list="$@"
if [ -z "$test_list" ]; then
  test_list="../src/*"
fi

# using tmpfs for /cvmfs mountpoint as / is readonly in virtme
sudo mount -t tmpfs -o size=512M cvmfs_root /cvmfs
# mounting cache disk
sudo mount /dev/vda /var/lib/cvmfs

echo "Running tests from: $test_list"
echo "Sourcing test_functions"
source ./test_functions

# Loop through each test and execute it
for test in $test_list; do
    if [ -d "$test" ] && [ -f "$test/main" ]; then
        test_name=$(basename "$test")

        # Skip if test is excluded
        if contains "$exclusions" "$test"; then
            echo "--Skipping $test_name (excluded)..." >> "$LOGFILE"
            echo "--Skipping $test_name (excluded)..."
            continue
        fi

        # Skip if test doesn't match label (if labels specified)
        t=$test
        if ! is_in_suite $test $labels; then
            echo "--Skipping $test_name (suite not selected)..." >> $LOGFILE
            echo "--Skipping $test_name (suite not selected)..."
            continue
        fi

        timestamp=$(date +"%Y-%m-%d %H:%M:%S")
        echo "[$timestamp] Running test: $test_name on kernel $KERNEL_VERSION" >> "$LOGFILE"

        bash -c "
            source ./test_functions
            cd ../
            test=${test/../.}
            source \$test/main
            cvmfs_run_test
            retval=\$?
            exit \$retval
        " >> "$LOGFILE" 2>&1
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
