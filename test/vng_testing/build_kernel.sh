#!/bin/bash
set -e

usage() {
    echo "Usage: $0 <linux-source-path> [<git-tag-or-commit>]"
    echo "    <linux-source-path> - Path to the Linux kernel source directory."
    echo "    <git-tag-or-commit> - Optional git tag or commit to checkout (e.g., v5.12)."
    echo "                          If omitted, you will be prompted interactively."
    echo "    The script will build the bzImage and place it in kernel/vx.xx/ directory."
    echo "    It also provides options to add/remove kernel config options and select a git tag or commit."
    exit 0
}

if [ $# -lt 1 ]; then
    usage
fi

SCRIPT_DIR=$(realpath "$(dirname "$0")")

# Set the Linux source directory
LINUX_SRC_PATH="$1"
if [ ! -d "$LINUX_SRC_PATH" ]; then
    echo "Error: Directory '$LINUX_SRC_PATH' does not exist."
    exit 1
fi

cd "$LINUX_SRC_PATH" || exit 1
if [ ! -d ".git" ]; then
    echo "Error: The directory '$LINUX_SRC_PATH' is not a valid git repository."
    exit 1
fi

# Accept tag from argument or prompt interactively
if [ -n "${2:-}" ]; then
    GIT_TAG_COMMIT="$2"
else
    echo "Enter the git tag or commit hash to checkout (e.g., v5.10 or a commit hash):"
    read -r GIT_TAG_COMMIT
fi
git checkout "$GIT_TAG_COMMIT" || { echo "Error: Git checkout failed"; exit 1; }

# Build the kernel using vng --build.  It generates a minimal .config
# automatically (via vng --kconfig) and compiles bzImage.
# WERROR=0 is needed because older kernels (< 5.18) trigger false-positive
# -Werror=use-after-free in objtool/libsubcmd when built with GCC >= 12.
echo "Building the kernel with vng --build..."
CONFIG_FILE="$SCRIPT_DIR/configs/kernel_config.conf"
VNG_CONFIG_ARGS=""
if [ -f "$CONFIG_FILE" ]; then
    VNG_CONFIG_ARGS="--config $CONFIG_FILE"
fi
WERROR=0 vng --build $VNG_CONFIG_ARGS \
    || { echo "Error: Kernel build failed."; exit 1; }

# Create the directory for storing the kernel.
# Use the git tag/commit as the directory name when provided, otherwise fall
# back to the version reported by the kernel Makefile.
KERNEL_VERSION="${GIT_TAG_COMMIT:-$(make kernelversion)}"
DEST_DIR="$SCRIPT_DIR/kernel/$KERNEL_VERSION"

# Ensure the target directory exists
mkdir -p "$DEST_DIR"

# Move the generated bzImage to the desired location
echo "Placing bzImage in '$DEST_DIR/'"
cp arch/x86/boot/bzImage "$DEST_DIR/"

echo "Kernel build complete!"
echo "bzImage has been placed in $DEST_DIR/"
