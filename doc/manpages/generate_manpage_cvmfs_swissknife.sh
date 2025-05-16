#!/bin/bash
docdir="$1"
outdir=$2
builddir=$3
 help2man \
 -i $docdir/cvmfs_common.h2m \
 --no-info \
 --section=1 \
 --output=$outdir/cvmfs_swissknife.1 \
 --name "cvmfs_swissknife: low-level tools for publishing and managing CernVM-FS repositories" \
 "bash -c '$builddir/cvmfs_swissknife|sed "s/$/\\\\n/"'"

