#!/bin/sh

#
# This script runs build scripts from the ci directory inside a specified docker
# container in the ci/docker directory.
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/common.sh

if [ $# -lt 3 ]; then
  echo "Usage: $0 <workspace>" "<docker image name>"
  echo "<build script invocation>"
  echo
  echo "This script runs a build script inside a docker container. The docker "
  echo "image is generated on demand - i.e. look into ci/docker for available"
  echo "docker image blueprints."
  exit 1
fi

CVMFS_WORKSPACE="$1"
CVMFS_DOCKER_IMAGE="$2"
shift 2

# retrieves the image creation time of a docker image in epoch
# @param image_name  the full name of the docker image
# @return            creation time in Unix epoch
image_creation() {
  local image_name="$1"
  date +%s --date "$(sudo docker inspect --format='{{.Created}}' $image_name)"
}

_time_from_git() {
  local relative_path="$1"
  date +%s --date "$(git log -1 --format=%ai -- $relative_path)"
}

_max() {
  local lhs="$1"
  local rhs="$2"
  [ $lhs -gt $rhs ] && echo $lhs || echo $rhs
}

_max_time_from_git() {
  local directory_path="$1"
  local max_epoch=0
  for f in $(find $directory_path -mindepth 1); do
    max_epoch="$(_max $max_epoch $(_time_from_git $f))"
  done
  echo $max_epoch
}

# retrieves the last-changed timestamp for a specific docker image recipe
# @param recipe_dir  the directory of the docker image recipe to check
# @return            last modified timestamp in Unix epoch
image_recipe() {
  local recipe_dir="$1"
  local owd="$(pwd)"
  cd ${recipe_dir}
  local recipe_epoch="$(_max_time_from_git .)"
  cd ..
  for d in $(find . -maxdepth 1 -mindepth 1 -type d -name '*_common'); do
    recipe_epoch="$(_max $recipe_epoch $(_max_time_from_git $d))"
  done
  cd $owd
  echo $recipe_epoch
}

bootstrap_image() {
  local image_name="$1"
  local container_dir="$2"

  echo "bootstrapping docker image for ${image_name}..."
  build_workdir=$(mktemp -d)
  old_wordir=$(pwd)
  cd $build_workdir
  cp ${container_dir}/* .
  [ -x ${container_dir}/build.sh ] || die "./build.sh not available or not executable"
  sudo ${container_dir}/build.sh   || die "Failed to build chroot tarball"
  sudo docker build --tag=$image_name .
  [ $? -eq 0 ] || die "Failed to build docker image '$image_name'"
  cd $old_wordir
  rm -fR $build_workdir
}

# check if docker is installed
which docker > /dev/null 2>&1 || die "docker is not installed"
which git    > /dev/null 2>&1 || die "git is not installed"

# check if the docker container specification exists in ci/docker
image_name="cvmfs/${CVMFS_DOCKER_IMAGE}"
container_dir="${SCRIPT_LOCATION}/docker/${CVMFS_DOCKER_IMAGE}"
[ -d $container_dir ] || die "container $CVMFS_DOCKER_IMAGE not found"

# bootstrap the docker container if non-existent or recreate if outdated
if ! sudo docker images $image_name | grep -q "$image_name"; then
  bootstrap_image "$image_name" "$container_dir"
elif [ $(image_creation $image_name) -lt $(image_recipe $container_dir) ]; then
  echo -n "removing outdated docker image '$image_name'... "
  sudo docker rmi -f "$image_name" > /dev/null || die "fail"
  echo "done"
  bootstrap_image "$image_name" "$container_dir"
fi

build_script="$1"
shift 1

[ -f $build_script ] || die "build script $build_script not found"
[ -x $build_script ] || die "build script $build_script not executable"

# parse the command line arguments (keep quotation marks)
while [ $# -gt 0 ]; do
  if echo "$1" | grep -q "[[:space:]]"; then
    args="$args \"$1\""
  else
    args="$args $1"
  fi
  shift 1
done

# run provided script inside the docker container
uid=$(id -u)
gid=$(id -g)
echo "++ $build_script $args"
sudo docker run --volume=${CVMFS_WORKSPACE}:${CVMFS_WORKSPACE}        \
                --volume=/etc/passwd:/etc/passwd                      \
                --volume=/etc/group:/etc/group                        \
                --user=${uid}:${gid}                                  \
                --rm=true                                             \
                --privileged=true                                     \
                --env="CVMFS_BUILD_ARCH=$CVMFS_BUILD_ARCH"            \
                --env="CVMFS_CI_PLATFORM_LABEL=$CVMFS_DOCKER_IMAGE"   \
                $image_name                                           \
                $build_script $args
