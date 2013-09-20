#!/bin/sh

set -e

script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common.sh
. ${script_location}/../test_functions

# parse script parameters
if [ $# -ne 1 ]; then
  echo "Usage: $0 <path to testee package>"
  exit 1
fi

# check availability of packages
current_package=$1
if [ ! -f $current_package ]; then
  echo "Package $current_package not found!"
  exit 1
fi

# download upstream package
current_version=$(package_version $current_package)
echo "Version of testee:        $current_version"
previous_version=$(decrement_version $current_version)
echo "Version to be downloaded: $previous_version"
upstream_package_url=$(guess_package_url "cvmfs" ${previous_version}-1)
echo "Download URL:             $upstream_package_url"
wget --quiet --no-check-certificate $upstream_package_url || die "failed to download upstream CernVM-FS"
upstream_package=$(basename $upstream_package_url)
echo "Upstream Package:         $upstream_package"

# make sure that there is no version of CernVM-FS installed
if is_installed "cvmfs"; then
  installed_version=$(installed_package_version "cvmfs")
  echo -n "uninstalling CernVM-FS $installed_version... "
  uninstall_package "cvmfs" || die "failed to remove CernVM-FS"
  echo "done"
fi

# install the upstream CernVM-FS package
echo -n "installing CernVM-FS $previous_version... "
install_package $upstream_package || die "failed to install upstream CernVM-FS"
echo "done"

# make sure that autofs is running
echo -n "switching on autofs... "
autofs_switch on  > /dev/null || die "failed to switch (1/3)"
autofs_switch off > /dev/null || die "failed to switch (2/3)"
autofs_switch on  > /dev/null || die "failed to switch (3/3)"
echo "done"

# make CernVM-FS ready to go
echo "setting up CernVM-FS (cvmfs_config setup)"
cvmfs_config setup || die "cvmfs_config setup failed"
cvmfs_config chksetup || die "cvmfs_config chksetup failed"

# mount a repository
echo "mounting sft.cern.ch"
cvmfs_mount sft.cern.ch

# do some hammering on the file system
echo "starting to tar the linux kernel (log output in separate file)"
linux_source=/cvmfs/sft.cern.ch/lcg/external/experimental/linux/linux-2.6.32.57
tar cvf kernel.tar.gz $linux_source > tar.log 2>&1 &
tar_pid=$!
echo "tar runs under PID $tar_pid"

# wait some time to bring tar up to speed
sleep 30

echo "==><><><><><><><><><><=="
echo "==> FUN STARTS HERE  <=="
echo "==><><><><><><><><><><=="

# do the CernVM-FS package update
echo -n "updating CernVM-FS package to version $current_version... "
install_package $current_package
echo "done"

# wait for the file system hammering to finish and collect the pieces
echo "waiting for tar to be finished... "
wait $tar_pid
tar_exit_code=$?
echo "done (exit code: $tar_exit_code)"

# all done
exit $tar_exit_code
