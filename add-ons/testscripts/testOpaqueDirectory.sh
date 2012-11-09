#!/bin/sh

die() {
  echo $1 >&2
  exit 1
}

REPO=dev.cern.ch
UNION=/cvmfs/$REPO

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

echo "creating some files"
echo "lol" > test1/lol
echo "rofl" > test1/test/test/rofl

cd

echo "commit this simple directory structure"
cvmfs_server publish || die "failed to publish"

cvmfs_server transaction || die "failed to init next transaction"

cd $UNION

echo "delete the directory structure and recreate it"
echo "  (this will create an opaque directory)"
rm -fR test1

mkdir test1
mkdir test1/test
mkdir test1/test/test
mkdir test1/test/test/test

echo "creating some files"
echo "lol" > test1/lol
echo "rofl" > test1/test/test/rofl

echo "commit the opaque directory and see what happens"
# normally the repository should look exactly the same afterwards!

cd

if [ $# -eq 0 ]
then
  cvmfs_server publish $REPO -d || die "failed to publish"
fi

echo "all done"
