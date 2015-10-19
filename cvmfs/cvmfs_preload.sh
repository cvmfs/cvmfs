#!/bin/bash
set -e
set -x

export TMPDIR=`mktemp -d /tmp/cvmfs_preload_selfextracted.XXXXXX`

ARCHIVE=`awk '/^__ARCHIVE_BELOW__/ {print NR + 1; exit 0; }' $0`

tail -n+"$ARCHIVE" "$0" | tar xzv -C "$TMPDIR"

CDIR=$(pwd)
cd "$TMPDIR"

# execute the command
LD_PRELOAD="$TMPDIR/libtbb_cvmfs.so.2 $TMPDIR/libtbbmalloc_cvmfs.so.2" ${TMPDIR}/cvmfs_preload $@

cd "$CDIR"
rm -rf "$TMPDIR"

exit 0

__ARCHIVE_BELOW__
