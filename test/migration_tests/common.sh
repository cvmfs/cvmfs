
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
    local slc_major_version=$(lsb_release --description --short | sed 's/^.* \([0-9]\)\.[0-9] .*$/\1/')
    local architecture=$(uname -m)
    if [ x"$slc_major_version" = x"5" ] || [ x"$slc_major_version" = x"6" ]; then
      if [ x"$slc_major_version" = x"5" ] && [ x"$architecture" = x"i686" ]; then
        architecture="i386"
      fi
      package_file_name="${package_name}-${cvmfs_version_string}.el${slc_major_version}.${architecture}.rpm"
    fi

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


installed_package_version() {
  local pkg_name=$1

  if has_binary yum; then
    yum info $pkg_name 2>/dev/null | grep -e "^Version" | sed 's/Version.*: \(.*\)$/\1/'
  elif has_binary dpkg; then
    dpkg --status $pkg_name 2>/dev/null | grep -e "^Version:" | sed 's/^Version: \([0-9]\.[0-9]\.[0-9]*\).*$/\1/'
  else
    return 1
  fi
}


is_installed() {
  local pkg_name=$1

  if has_binary yum; then
    yum info $pkg_name > /dev/null
  elif has_binary dpkg; then
    dpkg --status $pkg_name > /dev/null
  else
    return 1
  fi
}


install_package() {
  local pkg_path=$1

  if has_binary yum; then
    sudo yum -y install --nogpgcheck $pkg_path
  elif has_binary dpkg; then
    sudo dpkg --install $pkg_path
    sudo apt-get --assume-yes -f install
  else
    return 1
  fi
}


uninstall_package() {
  local pkg_name=$1

  if has_binary yum; then
    sudo yum -y erase $pkg_name
  elif has_binary apt-get; then
    sudo dpkg --remove $pkg_name
  else
    return 1
  fi
}


#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#


# check availability of packages
if [ x$CVMFS_CLIENT_PACKAGE = x"" ] || [ ! -f $CVMFS_CLIENT_PACKAGE ]; then
  echo "CernVM-FS client package '$CVMFS_CLIENT_PACKAGE' not found!"
fi

if [ x$CVMFS_SERVER_PACKAGE = x"" ] || [ ! -f $CVMFS_SERVER_PACKAGE ]; then
  echo "CernVM-FS server package '$CVMFS_SERVER_PACKAGE' not found!"
fi
