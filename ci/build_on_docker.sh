#!/bin/sh

#
# This script runs build scripts from the ci directory inside a specified docker
# container in the ci/docker directory.
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/common.sh

if [ $# -lt 4 ]; then
  echo "Usage: $0 <CernVM-FS source directory> <build result location>"
  echo "<docker image name> <build script invocation with OPTIONAL parameters>"
  echo
  echo "This script runs a build script inside a docker container."
  echo
  echo "NOTE: Don't specify the source and build directory for the build script"
  echo "      this will be passed automatically. Any optional parameters may"
  echo "      be passed as usual."
  exit 1
fi

CVMFS_SOURCE_LOCATION="$1"
CVMFS_RESULT_LOCATION="$2"
CVMFS_DOCKER_IMAGE="$3"
shift 3

# check if docker is installed
which docker > /dev/null 2>&1 || die "docker is not installed"

# check if the docker container specification exists in ci/docker
image_name="cvmfs/${CVMFS_DOCKER_IMAGE}"
container_dir="${SCRIPT_LOCATION}/docker/${CVMFS_DOCKER_IMAGE}"
[ -d $container_dir ] || die "container $CVMFS_DOCKER_IMAGE not found"

# bootstrap the docker container if necessary
if ! sudo docker images $image_name | grep -q "$image_name"; then
  echo "bootstrapping docker image for ${image_name}..."
  build_workdir=$(mktemp -d)
  old_wordir=$(pwd)
  cd $build_workdir
  cp ${container_dir}/* .
  [ -x ${container_dir}/build.sh ]                       || die "./build.sh not available or not executable"
  ${container_dir}/build.sh ${CVMFS_DOCKER_IMAGE}.tar.gz || die "Failed to build chroot tarball"
  sudo docker build --tag=$image_name .
  [ $? -eq 0 ] || die "Failed to build docker image '$image_name'"
  cd $old_wordir
  rm -fR $build_workdir
fi

# parse the command line arguments (keep quotation marks)
# Note: By convention the build scripts are called like this:
#       ./script.sh <source location> <build location> <optional parameters>
#       Source and build location are dpendent on the docker environment and
#       are appended here accordingly
build_script="$1"
shift 1

docker_source_location="/srv/source"
docker_build_location="/srv/build"
docker_build_script="${docker_source_location}/ci/$(basename $build_script)"

[ -f $build_script ] || die "build script $build_script not found"
[ -x $build_script ] || die "build script $build_script not executable"

args="${docker_source_location} ${docker_build_location}"
while [ $# -gt 0 ]; do
  if echo "$1" | grep -q "[[:space:]]"; then
    args="$args \"$1\""
  else
    args="$args $1"
  fi
  shift 1
done

# run provided script inside the docker container
echo "++ $docker_build_script $args"
sudo docker run --volume=${CVMFS_SOURCE_LOCATION}:${docker_source_location} \
                --volume=${CVMFS_RESULT_LOCATION}:${docker_build_location}  \
                --privileged=true                                           \
                --env="CVMFS_BUILD_ARCH=$CVMFS_BUILD_ARCH"                  \
                $image_name                                                 \
                $docker_build_script $args
