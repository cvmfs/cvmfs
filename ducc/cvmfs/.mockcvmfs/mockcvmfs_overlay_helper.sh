#!/bin/bash

# Usage: ./apply_overlay.sh <upperdir> <destination>
# This script applies changes from an overlayfs upperdir to a destination folder
# Handles native overlayfs whiteouts (character devices) and opaque directories (xattr)

set -e

if [ $# -ne 2 ]; then
    echo "Usage: $0 <upperdir> <destination>"
    exit 1
fi

UPPERDIR="$1"
DESTDIR="$2"

if [ ! -d "$UPPERDIR" ]; then
    echo "Error: Upperdir '$UPPERDIR' does not exist or is not a directory"
    exit 1
fi

if [ ! -d "$DESTDIR" ]; then
    echo "Error: Destination directory '$DESTDIR' does not exist or is not a directory"
    exit 1
fi

# Function to check if a file is a whiteout
is_whiteout() {
    local file="$1"
    # Check if it's a character device with 0:0 device numbers
    if [ -c "$file" ]; then
        local devnum=$(stat -c '%t:%T' "$file")
        if [ "$devnum" = "0:0" ]; then
            return 0  # True, it is a whiteout
        fi
    fi
    return 1  # False, not a whiteout
}

# Function to check if a directory is opaque
is_opaque_dir() {
    local dir="$1"
    if [ -d "$dir" ]; then
        # Check for trusted.overlay.opaque xattr
        if getfattr -n trusted.overlay.opaque --only-values "$dir" 2>/dev/null | grep -q "y"; then
            return 0  # True, it is opaque
        fi
    fi
    return 1  # False, not opaque
}

# Function to process a directory
process_directory() {
    local upper_path="$1"
    local dest_path="$2"
    local rel_path="$3"

    # Create destination directory if it doesn't exist
    mkdir -p "$dest_path"

    # Check if directory is opaque
    if is_opaque_dir "$upper_path"; then
        echo "Found opaque directory: $rel_path"
        # Remove all files in destination that aren't in upperdir
        find "$dest_path" -mindepth 1 -maxdepth 1 | while read item; do
            base_name=$(basename "$item")
            if [ ! -e "$upper_path/$base_name" ]; then
                echo "  Removing $rel_path/$base_name (due to opaque dir)"
                rm -rf "$item"
            fi
        done
    fi

    # Process all entries in the upperdir
    find "$upper_path" -mindepth 1 -maxdepth 1 | while read item; do
        base_name=$(basename "$item")

        # Handle whiteouts
        if is_whiteout "$item"; then
            echo "Found whiteout: $rel_path/$base_name"

            # Remove the corresponding file or directory in destination
            if [ -e "$dest_path/$base_name" ]; then
                echo "  Removing $rel_path/$base_name"
                rm -rf "$dest_path/$base_name"
            fi
            continue
        fi

        # Handle regular files and directories
        if [ -d "$item" ] && [ ! -L "$item" ]; then
            # Recursively process subdirectories
            process_directory "$item" "$dest_path/$base_name" "$rel_path/$base_name"
        else
            # Copy file (or symlink)
            echo "Copying $rel_path/$base_name"
            cp -a "$item" "$dest_path/$base_name"
        fi
    done
}

# Start processing from the root
process_directory "$UPPERDIR" "$DESTDIR" ""

echo "Overlay changes have been applied successfully"

