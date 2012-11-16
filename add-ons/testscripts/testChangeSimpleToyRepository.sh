#!/bin/sh

die() {
  echo $1 >&2
  exit 1
}

REPO=dev.cern.ch
UNION=/cvmfs/$REPO

echo "now run test1 to create a testbed..."
sh testCreateSimpleToyRepository.sh

echo "start transaction... "
cvmfs_server transaction $REPO || die "fail"

cd $UNION

echo "delete some stuff"
rm -fR test1
rm a b c
rm hardlinkToFoo2

echo "recreate directory test1 to test opaque directory support"
mkdir test1

echo "put some stuff in test1"
mkdir test1/test
mkdir test1/test/test
echo "lol" > test1/test/test/rofl
ln test1/test/test/rofl test1/test/test/lol
ln -s test/lol test1/test/symlinkToTestLOL

cd

if [ $# -eq 0 ]
then
  cvmfs_server publish $REPO -D || die "failed to publish"
fi

echo "all done"
