#!/bin/bash
set -e

usage() {
    echo "Usage: $0 <linux-source-path>"
    echo "    <linux-source-path> - Path to the Linux kernel source directory."
    echo "    The script will build the bzImage and place it in kernel/vx.xx/ directory."
    echo "    It also provides options to add/remove kernel config options and select a git tag or commit."
    exit 0
}

if [ $# -lt 1 ]; then
    usage
fi

SCRIPT_DIR=$(realpath "$(dirname "$0")")

# Function to enable or disable config options
# This function uses the `scripts/config` utility from the kernel source
configure_kernel() {
    echo "Configuring kernel options..."

    vng --kconfig
    CONFIG_FILE="$SCRIPT_DIR/configs/kernel_config.conf"
    if [ ! -f "$CONFIG_FILE" ]; then
        echo "Error: Config file '$CONFIG_FILE' does not exist."
        exit 1
    fi

    awk -F '=' '
        # Skip comments and empty lines
        /^[[:space:]]*#/ { next }
        /^[[:space:]]*$/ { next }

        # Split the line by '=' and assign to CONFIG_OPTION and CONFIG_VALUE
        { 
            CONFIG_OPTION = $1
            CONFIG_VALUE = $2
            # Enable or disable based on the value (y/n)
            if (CONFIG_VALUE == "y") {
                print "Enabling " CONFIG_OPTION
                system("'"$LINUX_SRC_PATH"'/scripts/config --enable " CONFIG_OPTION)
            } else if (CONFIG_VALUE == "n") {
                print "Disabling " CONFIG_OPTION
                system("'"$LINUX_SRC_PATH"'/scripts/config --disable " CONFIG_OPTION)
            } else {
                print "Invalid value for " CONFIG_OPTION ": " CONFIG_VALUE
            }
        }
    ' "$CONFIG_FILE"
}

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

# Prompt for a tag or commit hash
echo "Enter the git tag or commit hash to checkout (e.g., v5.10 or a commit hash):"
read -r GIT_TAG_COMMIT
git checkout "$GIT_TAG_COMMIT" || { echo "Error: Git checkout failed"; exit 1; }

# Call the function to configure the kernel
configure_kernel

# Build the kernel (bzImage)
echo "Building the kernel (bzImage)..."
make -j"$(nproc)" bzImage || { echo "Error: Kernel build failed."; exit 1; }

# Create the directory for storing the kernel
KERNEL_VERSION=$(make kernelversion)
DEST_DIR="$SCRIPT_DIR/kernel/$KERNEL_VERSION"

# Ensure the target directory exists
mkdir -p "$DEST_DIR"

# Move the generated bzImage to the desired location
echo "Placing bzImage in '$DEST_DIR/'"
cp arch/x86/boot/bzImage "$DEST_DIR/"

echo "Kernel build complete!"
echo "bzImage has been placed in $DEST_DIR/"
