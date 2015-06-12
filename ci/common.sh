#!/bin/sh

die() {
  local msg="$1"
  echo "$msg"
  exit 1
}

complain() {
  local msg="$1"
  echo "$msg"
  exit 0
}

is_linux() {
  [ x"$(uname)" = x"Linux" ]
}

is_macos() {
  [ x"$(uname)" = x"Darwin" ]
}

get_cvmfs_version_from_cmake() {
  local source_directory="$1"
  cat ${source_directory}/CMakeLists.txt | grep '## CVMFS_VERSION' | awk '{print $3}'
}

get_cvmfs_git_revision() {
  local source_directory="$1"
  echo "$(cd $source_directory; git rev-parse HEAD | head -c16)"
}

get_repository_root() {
  local script_location=$(cd "$(dirname "$0")"; pwd)
  echo $(cd "${script_location}/.."; pwd)
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

generate_package_map() {
  local platform="$1"
  local client="$2"
  local server="$3"
  local unittests="$4"
  local config="$5"

  cat > pkgmap.${platform} << EOF
[$platform]
client=$client
server=$server
unittests=$unittests
config=$config
EOF
}

python_version() {
  python --version 2>&1 | grep -oh '[0-9]\+\.[0-9]\+.[0-9]\+'
}

check_python_module() {
  local module="$1"
  python -c "import re, ${module};                                           \
             print(re.compile('/__init__.py.*').sub('',${module}.__file__))" \
             > /dev/null 2>&1
}

# makes sure that a version is always of the form x.y.z
normalize_version() {
  local version_string="$1"
  while [ $(echo "$version_string" | grep -o '\.' | wc -l) -lt 2 ]; do
    version_string="${version_string}.0"
  done
  echo "$version_string"
}
version_major() { echo $1 | cut -d. -f1; }
version_minor() { echo $1 | cut -d. -f2; }
version_patch() { echo $1 | cut -d. -f3; }
prepend_zeros() { printf %05d "$1"; }
compare_versions() {
  local lhs="$(normalize_version $1)"
  local comparison_operator=$2
  local rhs="$(normalize_version $3)"

  local lhs1=$(prepend_zeros $(version_major $lhs))
  local lhs2=$(prepend_zeros $(version_minor $lhs))
  local lhs3=$(prepend_zeros $(version_patch $lhs))
  local rhs1=$(prepend_zeros $(version_major $rhs))
  local rhs2=$(prepend_zeros $(version_minor $rhs))
  local rhs3=$(prepend_zeros $(version_patch $rhs))

  [ $lhs1$lhs2$lhs3 $comparison_operator $rhs1$rhs2$rhs3 ]
}
