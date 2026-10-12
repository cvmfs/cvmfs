#!/bin/bash
docdir="$1"
outdir=$2
builddir=$3

# cvmfs_server prints its version by shelling out to cvmfs_swissknife
# (cvmfs_version_string -> __swissknife version), so generating this man page
# runs a binary from the tree being built.
#
# Two things have to be arranged for that to work, and neither was:
#
# 1. PATH. Without $builddir on it, cvmfs_server finds whatever cvmfs_swissknife
#    is installed on the build host -- or none at all -- and documents that
#    version instead of the one being built.
#
# 2. LD_LIBRARY_PATH. The just-built swissknife links against just-built shared
#    libraries that are not installed yet. Nothing sets
#    CMAKE_LIBRARY_OUTPUT_DIRECTORY, so each library is written into the binary
#    directory mirroring its SOURCE directory: libcvmfs_crypto ends up in
#    $builddir/crypto and libcvmfs_util in $builddir/util, not beside the
#    executables in $builddir.
#
# When this is missing the failure is quiet and convincing. The loader writes
# "cvmfs_swissknife: error while loading shared libraries: libcvmfs_crypto.so"
# to stderr, help2man discards stderr by default, cvmfs_version_string swallows
# the non-zero exit and returns an empty string -- and the build fails with
# "can't get `--help' info", naming neither the library nor the binary. Run
# with --no-discard-stderr the loader message turns up as the man page TITLE.
#
# The directory list is derived rather than written out, so adding a library in
# a new subdirectory cannot silently reintroduce this.
libdirs=$(find "$builddir" -name 'libcvmfs_*.so*' -exec dirname {} \; 2>/dev/null \
          | sort -u | paste -sd: -)

 PATH="$builddir:$PATH" \
 LD_LIBRARY_PATH="${libdirs}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
 help2man \
 -i $docdir/cvmfs_common.h2m \
 --no-info \
 --section=1 \
 --output=$outdir/cvmfs_server.1 \
 --name "cvmfs_server: tools for publishing and managing CernVM-FS repositories" \
 "bash -c '$builddir/cvmfs_server|sed "s/$/\\\\n/"'"
