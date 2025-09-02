#!/bin/bash
set -e

KERNEL_DIR="./kernel"
TEST_DIR="./tests"
DISK_PATH="${DISK_PATH:-./cvmfs.img}"
DISK_SIZE="${DISK_SIZE:-5}"

# All arguments after run.sh are captured here
EXTRA_ARGS=()
while [[ $# -gt 0 ]]; do
  EXTRA_ARGS+=("$1")
  shift
done

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

    # create cvmfs_cache.img if not already present
    if [ -f "$DISK_PATH" ]; then
        echo "Disk already exists at $DISK_PATH. Skipping creation"
    else
        echo "Creating disk image: $DISK_PATH (${DISK_SIZE}G)"
        dd if=/dev/zero of="$DISK_PATH" bs=1G count="$DISK_SIZE" status=none
        echo "Disk created successfully"
    fi

    # reformat cvmfs_cache.img
    mkfs.ext4 -F "$DISK_PATH" >/dev/null 2>&1
}

# Function to create and run the VM with virtme-ng
create_and_run_vm() {
    local bzImage="$1"
    local kernel_version="$2"
    shift 2
    local escaped_args=$(printf "'%q' " "${EXTRA_ARGS[@]}")

    echo "Booting VM with kernel: $kernel_version"
    vng \
    --run "$bzImage" \
    --force-9p \
    --rwdir=results \
    --disk "$DISK_PATH" \
    --network user \
    --user $(whoami) \
    --exec "
        bash -c './guest/run_tests.sh $kernel_version $escaped_args'
    "
}

# Boot VM and run tests
setup
for bzImage in "$KERNEL_DIR"/*/bzImage; do
    kernel_version=$(basename "$(dirname "$bzImage")")
    create_and_run_vm "$bzImage" "$kernel_version"
done

