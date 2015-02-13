#!/bin/sh

die() {
  local msg="$1"
  echo "$msg"
  exit 1
}

get_cvmfs_version_from_cmake() {
  local source_directory="$1"
  cat ${source_directory}/CMakeLists.txt | grep '## CVMFS_VERSION' | awk '{print $3}'
}

get_cvmfs_git_revision() {
  local source_directory="$1"
  echo "$(cd $source_directory; git rev-parse HEAD | head -c16)"
}
