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

is_suse() {
  [ -f /etc/SuSE-release ]
}

is_redhat() {
  [ -f /etc/redhat-release ]
}

get_redhat_version() {
  cat /etc/redhat-release | sed -e 's/^.* \([0-9]\+\)\..*$/\1/'
}

get_package_type() {
  which dpkg > /dev/null 2>&1 && echo "deb" && return 0
  which rpm  > /dev/null 2>&1 && echo "rpm" && return 0
  [ x"$(uname)" = x"Darwin" ] && echo "pkg" && return 0
  return 1
}

get_default_compiler_arch() {
  local compiler=""
  which gcc   > /dev/null 2>&1 && compiler="gcc"
  which clang > /dev/null 2>&1 && compiler="clang"
  [ ! -z $compiler ] && $compiler -dumpmachine | grep -ohe '^[^-]\+'
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

  # create a temp directory to tar up
  # old `git archive` versions don't support --prefix
  local tmpd=$(mktemp -d)
  mkdir ${tmpd}/${tar_name}
  cd $tmpd
  cp -R ${source_directory}/AUTHORS            \
        ${source_directory}/CMakeLists.txt     \
        ${source_directory}/COPYING            \
        ${source_directory}/CPackLists.txt     \
        ${source_directory}/ChangeLog          \
        ${source_directory}/INSTALL            \
        ${source_directory}/NEWS               \
        ${source_directory}/README             \
        ${source_directory}/InstallerResources \
        ${source_directory}/add-ons            \
        ${source_directory}/bootstrap.sh       \
        ${source_directory}/cmake              \
        ${source_directory}/config_cmake.h.in  \
        ${source_directory}/cvmfs              \
        ${source_directory}/doc                \
        ${source_directory}/externals          \
        ${source_directory}/keys               \
        ${source_directory}/mount              \
        ${source_directory}/test               \
        $tar_name
  tar czf $destination_path $tar_name || true
  local retval=$?
  cd ..
  rm -fR $tmpd

  return $retval
}

generate_package_map() {
  local platform="$1"
  local client="$2"
  local server="$3"
  local devel="$4"
  local unittests="$5"
  local config="$6"

  cat > pkgmap.${platform} << EOF
[$platform]
client=$client
server=$server
devel=$devel
unittests=$unittests
config=$config
EOF
}

get_number_of_cpu_cores() {
  if is_linux; then
    cat /proc/cpuinfo | grep -e '^processor' | wc -l
  elif is_macos; then
    sysctl -n hw.ncpu
  else
    echo "1"
  fi
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

_template_placeholders() {
  local template_path="$1"
  cat $template_path | grep -ohe '@[^@]\+@' | sort | uniq
}

_unwrap_placeholder() {
  local placeholder="$1"
  echo "$placeholder" | sed -e 's/^@\([^@]*\)@/\1/'
}

_unwrap_variable() {
  local var="$1"
  echo $(eval "echo \$$var")
}

_escape_slashes() {
  echo "$1" | sed -e 's/\//\\\//g'
}

# Expands placeholder strings in a template file using all shell variables in
# the caller's scope. Placeholders look like this: @VARIABLE_NAME@
#
# @param template_path  path to the template file to be expanded
# @return               content of $template_path with expanded placeholders
expand_template() {
  local template_path="$1"

  local tmp="$(cat $template_path)"
  for placeholder in $(_template_placeholders $template_path); do
    local var=$(_unwrap_placeholder $placeholder)
    local cont="$(_unwrap_variable $var)"
    [ ! -z $cont ] || die "\$$var for '$template_path' is empty!"
    tmp="$(echo "$tmp" | sed -e "s/$placeholder/$(_escape_slashes $cont)/g")"
  done

  echo "$tmp"
}
