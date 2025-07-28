#!/bin/bash
set -e

KERNEL_BZIMAGE="./kernel/v5.11/bzImage"
TEST_DIR="./tests"

# Check if the kernel bzImage exists
if [ ! -f "$KERNEL_BZIMAGE" ]; then
    echo "Error: Kernel bzImage not found at $KERNEL_BZIMAGE."
    exit 1
fi

# Check if the test directory exists
if [ ! -d "$TEST_DIR" ]; then
    echo "Error: Test directory not found at $TEST_DIR."
    exit 1
fi

setup() {
    mkdir -p results
}

# Function to create and run the VM with virtme-ng
create_and_run_vm() {
    echo "Creating VM with kernel: $KERNEL_BZIMAGE"
    vng \
    --run "$KERNEL_BZIMAGE" \
    --rwdir=results \
    --exec '
        ./run_tests.sh
    '
}

# Function to run tests in the test directory
create_test_script() {
    echo "Creating test script inside the VM..."

    # Create a script that will run all tests in $TEST_DIR
    cat << EOF > ./run_tests.sh
#!/bin/bash
RESULTS_DIR="./results/test_failures.log"
echo "Running tests from directory: $TEST_DIR"

echo "Sourcing test_functions"
source ./test_functions

# Loop through each test and execute it
for test in $TEST_DIR/*; do
    if [ -d "\$test" ] && [ -f "\$test/main" ]; then
        test_name=\$(basename "\$test")
        echo "Running test: \$test_name"

        # Run the test script
        source "\$test/main"
        cvmfs_run_test
        exit_code=\$?

        timestamp=\$(date +"%Y-%m-%d %H:%M:%S")
        # Check if the test passed
        if [ \$exit_code -eq 0 ]; then
            echo "Test \$test_name passed."
        else
            echo "Test \$test_name failed with exit code \$exit_code."
            echo "[\$timestamp] \$test_name : \$exit_code" >> "\$RESULTS_DIR"
        fi
    fi
done
EOF

    # Make the test script executable
    chmod +x ./run_tests.sh
}


# Boot VM and run tests
setup
create_test_script
create_and_run_vm
