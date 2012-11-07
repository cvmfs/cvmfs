#!/bin/sh

die() {
  echo $1 >&2
  exit 1
}

UNION=/cvmfs/dev.cern.ch

echo "create fresh directory..."
sh bootstrap.sh

echo "start transaction... "
cvmfs_server transaction || die "fail"

cd $UNION

echo "creating some directories"
mkdir test1
mkdir test1/test
mkdir test1/test/test
mkdir test1/test/test/test

mkdir test2
mkdir test2/test
mkdir test2/test/test
mkdir test2/tset 

echo "creating some files"
echo "lol" > test1/lol
echo "rofl" > test1/test/test/rofl

cd

echo "commit this simple directory structure"
cvmfs_server publish || die "failed to publish"

cvmfs_server transaction || die "failed to init next transaction"

cd $UNION

echo "delete the directory structure"
rm -fR test1

cd

if [ $# -eq 0 ]
then
  cvmfs_server publish || die "failed to publish"
fi

echo "all done"
