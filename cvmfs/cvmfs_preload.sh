#!/bin/bash
export TMPDIR=`mktemp -d /tmp/cvmfs_preload_selfextracted.XXXXXX`

ARCHIVE=`awk '/^__ARCHIVE_BELOW__/ {print NR + 1; exit 0; }' $0`

tail -n+"$ARCHIVE" "$0" | tar xzv -C "$TMPDIR" || echo "Couldn't extract the tar file"

CDIR=$(pwd)
cd "$TMPDIR"

# execute the command
LD_PRELOAD="$TMPDIR/libtbb_cvmfs.so.2 $TMPDIR/libtbbmalloc_cvmfs.so.2 $TMPDIR/libssl.so $TMPDIR/libcrypto.so $TMPDIR/libgssapi_krb5.so $TMPDIR/libkrb5.so $TMPDIR/libk5crypto.so $TMPDIR/libkrb5support.so $TMPDIR/libkeyutils.so" $TMPDIR/cvmfs_preload $@ || echo "Failed to execute cvmfs_preload"

cd "$CDIR"
rm -rf "$TMPDIR"

exit 0

__ARCHIVE_BELOW__
