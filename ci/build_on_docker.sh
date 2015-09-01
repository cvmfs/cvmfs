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

# do special tricks for certain docker containers
if [ x"$CVMFS_DOCKER_IMAGE" = x"el4" ]; then
  el4_base="cvmfs/el4_base"
  if ! sudo docker images $el4_base | grep -q "$el4_base"; then
    el4_image="${container_dir}/centos49.tar.gz"
    sudo docker run --privileged                 \
                    --volume=$container_dir:/srv \
                    centos:centos6               \
                    /srv/build.sh
    [ $? -eq 0 ]      || die "failed to build CentOS 4 container image"
    [ -f $el4_image ] || die "didn't find '${el4_image}' after building"
    cat $el4_image | sudo docker import - $el4_base
    [ $? -eq 0 ] || die "failed to import just built el4 image ($el4_image)"
    rm -f $el4_image
  fi
fi

# make sure the docker container to be used is built and ready to go
sudo docker build --tag="$image_name" $container_dir
[ $? -eq 0 ] || die "failed to build $CVMFS_DOCKER_IMAGE ($container_dir)"

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
