
CVMFS_EC_BASE_URL="https://ecsft.cern.ch/dist/cvmfs"

# tries to guess the pacakge download URLs for a provided package name and cvmfs
# version. This relies on the Electric Commander installation of SFT.
#
# Note: CVMFS_EC_BASE_URL needs to point to the cvmfs download location
#
# @param package_name          the package to download (cvmfs | cvmfs-server)
# @param cvmfs_version_string  package version to be downloaded (fe. 2.1.15-1)
guess_package_url() {
  local package_name=$1
  local cvmfs_version_string=$2

  local short_cvmfs_version_string=$(echo "$cvmfs_version_string" | cut -d- -f1)
  local package_file_name=""

  # check that we are on Linux
  if [ $(uname -s 2>/dev/null) != "Linux" ]; then
    return 1
  fi

  # Ubuntu
  if [ -f /etc/debian_version ]                               && \
     [ x$(lsb_release --id --short 2>/dev/null) = x"Ubuntu" ] && \
     [ $(uname -m) = "x86_64" ]; then
    package_file_name="${package_name}_${short_cvmfs_version_string}_amd64.deb"

  # SLC 5 and 6
  elif [ x$(lsb_release --id --short 2>/dev/null) = x"ScientificCERNSLC" ]; then
    local slc_major_version=$(lsb_release --description --short | sed 's/^.* \([0-9][0-9]*\)\.[0-9][0-9]* .*$/\1/')
    local architecture=$(uname -m)
    if [ x"$slc_major_version" = x"5" ] || [ x"$slc_major_version" = x"6" ]; then
      if [ x"$slc_major_version" = x"5" ] && [ x"$architecture" = x"i686" ]; then
        architecture="i386"
      fi
      package_file_name="${package_name}-${cvmfs_version_string}.el${slc_major_version}.${architecture}.rpm"
    fi

  # CentOS 7
  elif [ x$(lsb_release --id --short 2>/dev/null) = x"CentOS" ]; then
    local slc_major_version=$(lsb_release --description --short | sed 's/^.* \([0-9][0-9]*\)\.[0-9\.][0-9\.]* .*$/\1/')
    local architecture=$(uname -m)
    package_file_name="${package_name}-${cvmfs_version_string}.el${slc_major_version}.centos.${architecture}.rpm"

  # to be extended
  else
    return 2
  fi

  # check if we found something useful
  if [ x"$package_file_name" = x"" ]; then
    return 3
  fi

  # build and echo the full rpm URL
  echo "${CVMFS_EC_BASE_URL}/cvmfs-${short_cvmfs_version_string}/${package_file_name}"
  return 0
}


# makes sure that a version is always of the form x.y.z
normalize_version() {
  local version_string="$1"
  while [ $(echo "$version_string" | grep -o '\.' | wc -l) -lt 2 ]; do
    version_string="${version_string}.0"
  done
  echo "$version_string"
}
version_major() { echo $1 | cut --delimiter=. --fields=1; }
version_minor() { echo $1 | cut --delimiter=. --fields=2; }
version_patch() { echo $1 | cut --delimiter=. --fields=3; }
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
version_lower_or_equal() {
  local testee=$1
  local compare=$2
  compare_versions $testee -le $compare
}


has_binary() {
  local binary_name=$1
  which $binary_name > /dev/null 2>&1
}


decrement_version() {
  local input_version=$1
  local patch_number
  local base_version

  patch_number=$(echo $input_version | cut --delimiter=. --fields=3)
  base_version=$(echo $input_version | sed 's/^\([0-9]\.[0-9]\)\..*$/\1/')
  patch_number=$(( $patch_number - 1 ))
  echo "$base_version.$patch_number"
}


package_version() {
  local pkg_path=$1

  if has_binary rpm; then
    rpm -qp --queryformat='%{VERSION}' $pkg_path 2>/dev/null
  elif has_binary dpkg; then
    dpkg --info $pkg_path 2>/dev/null | grep -e "^ Version:" | sed 's/^ Version: \([0-9]\.[0-9]\.[0-9]*\).*$/\1/'
  else
    return 1
  fi
}


get_providing_packages() {
  local virt_pkg_name=$1

  if has_binary rpm; then
    rpm -q --whatprovides $virt_pkg_name
  elif has_binary dpkg; then
    dpkg -l | grep $virt_pkg_name | awk '{print $2}'
  else
    return 1
  fi
}


installed_package_version() {
  local pkg_name=$1

  if has_binary rpm; then
    echo $(rpm -qa --queryformat '%{version}' yum)
  elif has_binary dpkg; then
    dpkg --status $pkg_name 2>/dev/null | grep -e "^Version:" | sed 's/^Version: \([0-9]\.[0-9]\.[0-9]*\).*$/\1/'
  else
    return 1
  fi
}


is_installed() {
  local pkg_name=$1

  if has_binary dnf; then
    dnf info $pkg_name > /dev/null
  elif has_binary yum; then
    yum info $pkg_name > /dev/null
  elif has_binary dpkg; then
    dpkg --status $pkg_name > /dev/null
  else
    return 1
  fi
}


yum_install_packages() {
  local pkg_paths="$1"
  sudo yum -y install --nogpgcheck $pkg_paths
}


dnf_install_packages() {
  local pkg_paths="$1"
  sudo dnf -y install --nogpgcheck $pkg_paths
}


yum_get_package_name() {
  local rpm_file="$1"
  echo $(rpm -qp --queryformat '%{NAME}' $rpm_file)
}


yum_get_conflicts() {
  local rpm_file="$1"
  local conflicts="$(rpm -qp --queryformat '%{CONFLICTS}' $rpm_file)"
  if echo "$conflicts" | grep -vq '(none)'; then
    echo $conflicts
  fi
}


yum_update_fallback() {
  local pkg_paths="$1"
  local to_be_uninstalled=""

  for pkg in $pkg_paths; do
    local pkg_name="$(yum_get_package_name $pkg)"
    if is_installed $pkg_name; then
      to_be_uninstalled="$pkg_name $to_be_uninstalled"
    fi
    to_be_uninstalled="$(yum_get_conflicts $pkg) $to_be_uninstalled"
  done

  if [ ! -z "$to_be_uninstalled" ]; then
    uninstall_package "$to_be_uninstalled"
  fi
  yum_install_packages "$pkg_paths"
}


install_packages() {
  local pkg_paths="$1"

  if has_binary dnf; then
    dnf_install_packages "$pkg_paths"
  elif has_binary yum; then
    if version_lower_or_equal $(installed_package_version 'yum') '3.2.22'; then
      yum_update_fallback "$pkg_paths"
    else
      yum_install_packages "$pkg_paths"
    fi
  elif has_binary dpkg; then
    sudo dpkg --install $pkg_paths
    sudo apt-get --assume-yes -f install
  else
    return 1
  fi
}


uninstall_package() {
  local pkg_names="$1"

  if has_binary dnf; then
    sudo dnf -y erase $pkg_names
  elif has_binary yum; then
    sudo yum -y erase $pkg_names
  elif has_binary apt-get; then
    sudo apt-get --assume-yes purge $pkg_names
  else
    return 1
  fi
}


#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#


# check availability of packages
if [ x"$CVMFS_CLIENT_PACKAGE" = x"" ] || [ ! -f $CVMFS_CLIENT_PACKAGE ]; then
  echo "CernVM-FS client package '$CVMFS_CLIENT_PACKAGE' not found!"
fi

if [ x"$CVMFS_SERVER_PACKAGE" = x"" ] || [ ! -f $CVMFS_SERVER_PACKAGE ]; then
  echo "CernVM-FS server package '$CVMFS_SERVER_PACKAGE' not found!"
fi

if [ x"$CVMFS_CONFIG_PACKAGE" = x"" ] || [ ! -f $CVMFS_CONFIG_PACKAGE ]; then
  echo "CernVM-FS config package '$CVMFS_CONFIG_PACKAGE' not found!"
fi
