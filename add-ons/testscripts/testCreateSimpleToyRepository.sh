#!/bin/sh

die() {
  echo $1 >&2
  exit 1
}

REPO=dev.cern.ch
UNION=/cvmfs/$REPO

echo "recreate the repo first..."
sh recreateRepo.sh

echo "start transaction... "
cvmfs_server transaction || die "fail"

cd $UNION

echo "creating some directories"
mkdir test1
mkdir test2
mkdir test3
mkdir test4

mkdir test1/test
mkdir test1/test/test
mkdir test1/test/test/test

mkdir test2/foo
mkdir test2/foo/bar

echo "creating some files"
touch a b c d
echo "lol" > lol
echo "moep" > moep
touch test1/lol
echo "lol" > foo
echo "rofl" > test1/test/test/rofl

echo "creating symlinks"
ln -s test1 symlinkToTest1
ln -s foo symlinkToFoo

echo "creating hardlinks"
ln foo hardlinkToFoo
ln foo hardlinkToFoo2
ln a hardlinkToA

cd

if [ $# -eq 0 ]
then
  cvmfs_server publish $REPO -d || die "failed to publish"
fi

echo "all done"
