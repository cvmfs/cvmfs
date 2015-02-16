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

create_cvmfs_source_tarball() {
  local source_directory="$1"
  local destination_path="$2"
  local prev_dir="$(pwd)"

  # sanity check
  echo "$destination_path" | grep -q '\.tar\.gz$' || die "'$destination_path' should be a .tar.gz file"

  # figure out the tar archive prefix
  local tar_name
  tar_name="$(basename $destination_path | sed -e 's/\(.*\)\.tar\.gz$/\1/')"

  cd "$source_directory"
  git archive --prefix ${tar_name}/ \
              --format tar          \
                                    \
              HEAD                  \
              AUTHORS               \
              CMakeLists.txt        \
              COPYING               \
              CPackLists.txt        \
              ChangeLog             \
              INSTALL               \
              NEWS                  \
              README                \
              InstallerResources    \
              add-ons               \
              bootstrap.sh          \
              cmake                 \
              config_cmake.h.in     \
              cvmfs                 \
              doc                   \
              externals             \
              keys                  \
              mount                 \
              test | gzip -c > $destination_path || true
  local retval=$?
  cd "$prev_dir"

  return $retval
}
