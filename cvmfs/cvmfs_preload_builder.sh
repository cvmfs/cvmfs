#!/bin/bash

# copy all the shared libraries
tmpdir="/tmp/cvmfs_preload_tempdir"
old_dir=$(pwd)
bin_dir="$1"
src_dir="$2"

mkdir "$tmpdir"
ldd "$bin_dir/cvmfs_preload_bin" | grep "=> /" | awk '{print $3}' | xargs -I '{}' cp -v '{}' "$tmpdir"
cp "$bin_dir/cvmfs_preload_bin" "$tmpdir"

# compress the files
cd "$tmpdir"
tar czf cvmfs_preload.tar.gz *

# combine both files
cat "$src_dir/cvmfs_preload" ./cvmfs_preload.tar.gz > "$bin_dir/cvmfs_preload"
chmod +x "$bin_dir/cvmfs_preload"

# delete the temporary directory
cd "$old_dir"
rm -rf "$tmpdir"
