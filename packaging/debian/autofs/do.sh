#!/bin/sh

set -e

#
# This script is not called by the CI system! It is supposed to be used for
# package creation debugging and as a blue print for CI configuration.
#

usage() {
  echo "Build the Debian stretch autofs version for older deb based distros"
  echo "Usage: $0 <work dir>"
  exit 1
}

if [ $# -ne 1 ]; then
  usage
fi

workdir=$1

if [ "$(ls -A $workdir 2>/dev/null)" != "" ]; then
  echo "$workdir must be empty"
  exit 2
fi

echo -n "creating workspace in $workdir... "
mkdir ${workdir}/src ${workdir}/result
echo "done"

echo -n "getting autofs source package"
cd ${workdir}/src
apt-get source autofs/stretch
srcdir=$(find . -mindepth 1 -maxdepth 1 -type d)
cd $srcdir
dpkg-checkbuilddeps
dch --local cernvm "rebuild stretch autofs for recursive mounting support"
echo "done"

debuild -us -uc
mv ${workdir}/src/autofs_*cernvm* ${workdir}/result
