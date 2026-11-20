#!/bin/bash
docdir="$1"
outdir=$2
builddir=$3
# cvmfs_server gets its version from "cvmfs_swissknife" on PATH; use the built one
PATH="$builddir:$PATH" \
 help2man \
 -i $docdir/cvmfs_common.h2m \
 --no-info \
 --section=1 \
 --output=$outdir/cvmfs_server.1 \
 --name "cvmfs_server: tools for publishing and managing CernVM-FS repositories" \
 "bash -c '$builddir/cvmfs_server|sed "s/$/\\\\n/"'"
