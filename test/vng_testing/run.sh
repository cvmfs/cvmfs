#!/bin/bash
set -e

KERNEL_DIR="./kernel"
TEST_DIR="./tests"

# Check if the kernel directory exists
if [ ! -d "$KERNEL_DIR" ]; then
    echo "Error: Kernel directory not found at $KERNEL_DIR."
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
    local bzImage="$1"
    local kernel_version="$2"

    echo "Creating test script for kernel $kernel_version"
    create_test_script "$kernel_version"

    echo "Booting VM with kernel: $kernel_version"
    vng \
    --run "$bzImage" \
    --rwdir=results \
    --exec '
        ./run_tests.sh
    '
}

# Function to run tests in the test directory
create_test_script() {
    local kernel_version="$1"

    # Create a script that will run all tests in $TEST_DIR
    cat << EOF > ./run_tests.sh
#!/bin/bash
RESULTS_DIR="./results/test_failures.log"
KERNEL_VERSION="__KERNEL_VERSION__"

echo "Running tests from directory: $TEST_DIR"
echo "Sourcing test_functions"
source ./test_functions

# Loop through each test and execute it
for test in $TEST_DIR/*; do
    if [ -d "\$test" ] && [ -f "\$test/main" ]; then
        test_name=\$(basename "\$test")
        timestamp=\$(date +"%Y-%m-%d %H:%M:%S")
        echo "[\$timestamp] Running test: \$test_name on kernel \$KERNEL_VERSION"

        # Run the test script
        source "\$test/main"
        cvmfs_run_test
        exit_code=\$?

        timestamp=\$(date +"%Y-%m-%d %H:%M:%S")
        # Check if the test passed
        if [ \$exit_code -eq 0 ]; then
            echo "Test \$test_name passed on \$KERNEL_VERSION."
        else
            echo "Test \$test_name failed with exit code \$exit_code on \$KERNEL_VERSION."
            echo "[\$timestamp] [\$KERNEL_VERSION] \$test_name : \$exit_code" >> "\$RESULTS_DIR"
        fi
    fi
done
EOF

    sed -i "s/__KERNEL_VERSION__/$kernel_version/g" ./run_tests.sh
    chmod +x ./run_tests.sh
}


# Boot VM and run tests
setup
for bzImage in "$KERNEL_DIR"/*/bzImage; do
    kernel_version=$(basename "$(dirname "$bzImage")")
    create_and_run_vm "$bzImage" "$kernel_version"
done

