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

mkdir test2/
mkdir test2/test
mkdir test2/tset
mkdir test2/test/test
mkdir test2/tset/test
mkdir test2/test/test/test
mkdir test2/tset/test/test

echo "creating some files"
echo "lol" > test1/lol
echo "rofl" > test1/test/test/rofl
echo "lol" > test1/test/lol
echo "lol" > test1/test/test/lol
echo "rofl" > test1/test/roflcopter

echo "create a nested catalog mark at test1/test/.cvmfscatalog"
touch test1/test/.cvmfscatalog

cd

echo "commit to the repository"
cvmfs_server publish || die "failed to commit nested catalog structure to repositiory"

cvmfs_server transaction || die "failed to open new transaction"

cd $UNION

rm test1/test/.cvmfscatalog || die "no catalog found at expected position"

cd

echo "commit this simple directory structure"
if [ $# -eq 0 ]
then
  cvmfs_server publish $REPO -d || die "failed to publish"
fi

echo "all done"
