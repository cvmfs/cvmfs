#!/bin/bash
set -e

usage() {
    echo "Usage: $0 [--help] [-- <run.sh args>]"
    echo "    Boots a virtme-ng VM and runs test/run.sh inside it."
    echo "    All arguments after '--' are forwarded to test/run.sh."
    echo ""
    echo "    Example: $0 -- -s quick -x src/095-fuser -- src/00* src/01*"
    exit 0
}

if [ "${1:-}" = "--help" ]; then
    usage
fi

KERNEL_VERSION="v5.12"
SCRIPT_DIR=$(realpath "$(dirname "$0")")
TEST_DIR=$(realpath "$SCRIPT_DIR/..")
KERNEL_DIR="$SCRIPT_DIR/kernel"
KERNEL_BASE_URL=${KERNEL_BASE_URL:="https://ecsft.cern.ch/dist/cvmfs/caches/kernel/"}
# This disk serves as cvmfs cache
DISK_PATH="${DISK_PATH:-$SCRIPT_DIR/cvmfs.img}"
DISK_SIZE="${DISK_SIZE:-5}"

# All arguments after '--' are forwarded to test/run.sh
RUN_SH_ARGS=()
while [[ $# -gt 0 ]]; do
  if [ "$1" = "--" ]; then
    shift
    RUN_SH_ARGS=("$@")
    break
  fi
  shift
done

# Check if the kernel directory exists
if [ ! -d "$KERNEL_DIR" ]; then
    echo "Kernel directory not found at $KERNEL_DIR. Creating..."
    mkdir -p "$KERNEL_DIR"
fi

setup() {
    mkdir -p "$SCRIPT_DIR/results"

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

fetch_kernel() {
    if [ -f "$KERNEL_DIR/$KERNEL_VERSION/bzImage" ]; then
        echo "Kernel $KERNEL_VERSION already present at $KERNEL_DIR/$KERNEL_VERSION/bzImage, skipping fetch."
        return 0
    fi
    echo "Fetching kernel $KERNEL_VERSION from $KERNEL_BASE_URL ..."
    [ -d "$KERNEL_DIR/$KERNEL_VERSION" ] || mkdir -p "$KERNEL_DIR/$KERNEL_VERSION"
    if ! wget --quiet -O "$KERNEL_DIR/$KERNEL_VERSION/bzImage" "$KERNEL_BASE_URL/$KERNEL_VERSION/bzImage"; then
        echo "Error fetching kernel $KERNEL_VERSION from $KERNEL_BASE_URL"
        exit 1
    fi
    echo "Listing fetched kernels:"
    ls "$KERNEL_DIR"
}

fetch_kernels() {
    echo "Fetching kernels from $KERNEL_BASE_URL..."
    if ! wget --recursive --no-parent --no-host-directories --cut-dirs=4 --accept bzImage --quiet -P "$KERNEL_DIR" "$KERNEL_BASE_URL"; then
        echo "Error fetching kernels from $KERNEL_BASE_URL"
        exit 1
    fi
    echo "Listing fetched kernels:"
    ls "$KERNEL_DIR"
}

# Function to create and run the VM with virtme-ng
create_and_run_vm() {
    local bzImage="$1"
    local kernel_version="$2"
    shift 2

    # Build a properly quoted argument string for run.sh
    local quoted_args=""
    for arg in "${RUN_SH_ARGS[@]}"; do
        quoted_args+="$(printf " %q" "$arg")"
    done

    # Write the guest script to a file instead of passing it inline via
    # --exec.  The --exec payload is base64-encoded onto the kernel command
    # line, which is limited to 2048 bytes on x86 (COMMAND_LINE_SIZE).
    # With many test exclusions the encoded script exceeds that limit,
    # truncating the trailing init= argument and causing a kernel panic.
    # The host filesystem is mounted inside the VM via 9p, so the script
    # file is directly accessible.
    local guest_script
    guest_script=$(mktemp "$SCRIPT_DIR/guest-script.XXXXXX.sh")
    chmod +x "$guest_script"
    cat > "$guest_script" <<GUEST_EOF
#!/bin/bash
# VM-specific mounts: / is read-only in virtme, so use tmpfs for /cvmfs
sudo mount -t tmpfs -o size=512M cvmfs_root /cvmfs
sudo mount /dev/vda /var/lib/cvmfs

echo '=== VM System Info ==='
uname -a
echo '======================'

# 10.0.2.2 is the host gateway in QEMU SLIRP mode;
# DIRECT avoids needing squid on the host
export CVMFS_TEST_PROXY=DIRECT
# Route the cern.ch / egi.eu stratum 1s through the OpenHTC CDN to reduce
# network flakiness instead of hitting a single stratum 1 directly
export CVMFS_TEST_USE_CDN=yes
# Skip autofs/systemd checks — the VM has no init system
export CVMFS_TEST_DOCKER=yes

cd $TEST_DIR && ./run.sh$quoted_args
GUEST_EOF
    trap "rm -f '$guest_script'" EXIT

    echo "virtme-ng version: $(vng --version 2>&1)"
    echo "Booting VM with kernel: $kernel_version"
    echo "KVM available: $([ -w /dev/kvm ] && echo yes || echo no)"
    vng \
    --run "$bzImage" \
    --force-9p \
    --force-initramfs \
    --disk "$DISK_PATH" \
    --network user \
    --user "$(whoami)" \
    --verbose \
    --exec "$guest_script"
}

# Fetch kernels
fetch_kernel
# Boot VM and run tests
setup
for bzImage in "$KERNEL_DIR"/*/bzImage; do
    kernel_version=$(basename "$(dirname "$bzImage")")
    create_and_run_vm "$bzImage" "$kernel_version"
done

