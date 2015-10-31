#!/bin/bash

set -e

# copy all the shared libraries
tmpdir=`mktemp -d /tmp/cvmfs_preload_builder.XXXXXX`
old_dir=$(pwd)
bin_dir="$1"
src_dir="$2"

ldd "$bin_dir/cvmfs_preload_bin" | grep "=> /" | awk '{print $3}' | xargs -I '{}' cp -v '{}' "$tmpdir"
cp "$bin_dir/cvmfs_preload_bin" "$tmpdir"

# compress the files
cd "$tmpdir"
tar czf cvmfs_preload.tar.gz *

# combine both files
cat "$src_dir/cvmfs_preload" ./cvmfs_preload.tar.gz > "$bin_dir/cvmfs_preload"
rm -f ./cvmfs_preload.tar.gz
chmod +x "$bin_dir/cvmfs_preload"

# delete the temporary directory
cd "$old_dir"
rm -rf "$tmpdir"
