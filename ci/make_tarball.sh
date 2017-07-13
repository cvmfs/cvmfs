#!/bin/sh

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)

if [ $# -lt 1 ]; then
  echo "Usage: $0 <CernVM-FS source directory (the same as the build directory)>"
  echo "This script builds packages for the current platform."
  exit 1
fi

CVMFS_BUILD_LOCATION="$1"
shift 1

export REBAR_CACHE_DIR=$WORKSPACE

# run the build script
echo "switching to $CVMFS_BUILD_LOCATION..."
cd "$CVMFS_BUILD_LOCATION"
rebar3 as prod compile
cd _build/prod/lib/syslog
./rebar compile
cd -
rebar3 as prod release,tar
REPO_SERVICES_VERSION=$(grep -o "[0-9]\+\.[0-9]\+\.[0-9]\+" apps/cvmfs_services/src/cvmfs_services.app.src)
mkdir -p $CVMFS_BUILD_LOCATION/tarballs
cp -v _build/prod/rel/cvmfs_services/cvmfs_services-$REPO_SERVICES_VERSION.tar.gz \
   $CVMFS_BUILD_LOCATION/tarballs/cvmfs_services-$REPO_SERVICES_VERSION-$CVMFS_BUILD_PLATFORM-x86_64.tar.gz
