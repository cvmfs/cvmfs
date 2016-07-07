#!/bin/bash

export LC_ALL=C

script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/../../test_functions

# splits onelined CSV strings and prints the desired field offset
#
# @param cvs     the CSV string to be cut
# @param offset  the offset to be printed
get_csv_item() {
  local csv="$1"
  local offset=$2
  local delim=${3:-,}

  echo $csv | cut -d $delim -f $offset
}


strip_unit() {
  local literal=$1
  echo $literal | sed -e 's/^\([0-9][0-9]*\).*$/\1/'
}


machine_readable_legacy_parted() {
  local device=$1
  local p="sudo parted --script $device -- unit B print"
  local l1=$($p | head -n2 | tail -n1)
  local l2=$($p | head -n3 | tail -n1)
  local l3=$($p | head -n4 | tail -n1)
  local l4=$($p | head -n5 | tail -n1)
  local devtype=$(echo "$l1" | sed -e 's/^Model: \(.*\) (\(.*\)).*$/\1/')
  local virtblk=$(echo "$l1" | sed -e 's/^Model: \(.*\) (\(.*\)).*$/\2/')
  local devpath=$(echo "$l2" | sed -e 's/^Disk \(.*\): \([0-9]\+\).*$/\1/')
  local devsize=$(echo "$l2" | sed -e 's/^Disk \(.*\): \([0-9]\+.\).*$/\2/')
  local sector1=$(echo "$l3" | sed -e 's/^Sector .*: \([0-9]\+\).\/\([0-9]\+\).*$/\1/')
  local sector2=$(echo "$l3" | sed -e 's/^Sector .*: \([0-9]\+\).\/\([0-9]\+\).*$/\2/')
  local tablety=$(echo "$l4" | sed -e 's/^Partition Table: \(.*\)$/\1/')
  local lines=$($p | wc -l)
  local tailn=$(( $lines - 7 ))
  local headn=$(( $tailn - 1 ))

  echo "I don't know"
  echo "$devpath:$devsize:$virtblk:$sector1:$sector2:$tablety:$devtype;"
  $p | tail -n $tailn | head -n $headn  | sed -e 's/\s*\([0-9]\+\)\s\+\([0-9]\+.\)\s\+\([0-9]\+.\)\s\+\([0-9]\+.\)\s\+[a-z0-9]*\s\+\([a-z0-9]*\)\s\+\([a-z0-9]*\).*$/\1:\2:\3:\4:\5::\6/'
}


is_legacy_parted() {
  local major_version=$(sudo parted --version | head -n1 | sed -e 's/^.*\s\([0-9]\)\..*$/\1/')
  [ $major_version -lt 2 ]
}


get_partition_table() {
  local device=$1

  if is_legacy_parted; then
    machine_readable_legacy_parted $device
  else
    sudo parted --script --machine $device -- unit B print
  fi
}


get_last_partition_end() {
  local device=$1
  local last_part_stat

  last_part_stat=$(get_partition_table $device | tail -n1)
  strip_unit $(get_csv_item "$last_part_stat" 3 ":")
}


get_last_partition_number() {
  local device=$1
  local last_part_stat

  last_part_stat=$(get_partition_table $device | tail -n1)
  get_csv_item "$last_part_stat" 1 ":"
}


get_device_capacity() {
  local device=$1
  local dev_stats

  dev_stats=$(get_partition_table $device | head -n2 | tail -n1)
  strip_unit $(get_csv_item "$dev_stats" 2 ":")
}


get_unpartitioned_space() {
  local device=$1
  local dev_size
  local last_part_end
  dev_size=$(get_device_capacity $device)
  last_part_end=$(get_last_partition_end $device)

  echo "$(( $dev_size - $last_part_end ))"
}


create_partition_at() {
  local device=$1
  local p_start=$2
  local p_end=$3
  local p_type=$4

  local num_before
  num_before=$(get_last_partition_number $device)
  if is_legacy_parted; then
    sudo parted --script $device -- \
      unit B mkpart $p_type $p_start $p_end
  else
    sudo parted --script --machine --align optimal $device -- \
      unit B mkpart $p_type $p_start $p_end
  fi
  sudo partprobe
  [ $num_before -ne $(get_last_partition_number $device) ] # check if new partition appeared
}


create_partition() {
  local device=$1
  local p_size=$2
  local p_type=${3:-"primary"}
  local p_start
  local p_end

  local last_part_end
  last_part_end=$(get_last_partition_end $device)

  p_start=$(( $last_part_end + 1 ))
  p_end=$(( $p_start + $p_size ))

  create_partition_at $device $p_start $p_end $p_type
}


format_partition_ext4() {
  local partition_device=$1

  sudo mkfs.ext4 -q -N 10000000 $partition_device
}


mount_partition() {
  local partition_device=$1
  local mountpoint=$2

  sudo mkdir -p $mountpoint > /dev/null || return 1
  sudo mount -t ext4 $partition_device $mountpoint > /dev/null || return 2
  sudo rm -fR $mountpoint/lost+found > /dev/null || return 3
}


is_linux() {
  [ x"$(uname)" = x"Linux" ]
}


is_macos() {
  [ x"$(uname)" = x"Darwin" ]
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


#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#


rpm_name_string() {
  local rpm_file=$1
  echo $(rpm -qp --queryformat '%{NAME}' $rpm_file)
}


deb_name_string() {
  local deb_file=$1
  echo $(dpkg --info $deb_file | grep " Package: " | sed 's/ Package: //')
}


check_package_manager_response() {
  local retcode=$1
  local pkg_mgr_name="$2"
  local pkg_mgr_output="$3"

  if [ $retcode -ne 0 ]; then
    echo "fail"
    echo "$pkg_mgr_name said:"
    echo $pkg_mgr_output
    exit 102
  else
    echo "done"
  fi

  return $retcode
}


install_rpm() {
  local rpm_files="$1"
  local yum_output

  for this_rpm in $rpm_files; do
    local rpm_name=$(rpm_name_string $this_rpm)

    # check if the given rpm is already installed
    if rpm -q $rpm_name > /dev/null 2>&1; then
      echo "RPM '$rpm_name' is already installed"
      exit 101
    fi

    # install the RPM
    echo -n "Installing RPM '$rpm_name' ... "
    if which dnf > /dev/null 2>&1; then
      yum_output=$(sudo dnf -y install --nogpgcheck $this_rpm 2>&1)
      check_package_manager_response $? "DNF" "$yum_output"
    else
      yum_output=$(sudo yum -y install --nogpgcheck $this_rpm 2>&1)
      check_package_manager_response $? "Yum" "$yum_output"
    fi
  done
}


install_deb() {
  local deb_files="$1"
  local deb_output

  for this_deb in $deb_files; do
    local deb_name=$(deb_name_string $this_deb)

    # install DEB package
    echo -n "Installing DEB package '$deb_name' ... "
    deb_output=$(sudo gdebi --non-interactive --quiet $this_deb)
    check_package_manager_response $? "DPKG" "$deb_output"
  done
}


install_from_repo() {
  local package_names="$1"
  local pkg_mgr
  local pkg_mgr_output

  # find out which package manager to use
  if which apt-get > /dev/null 2>&1; then
    pkg_mgr="apt-get"
  elif which dnf > /dev/null 2>&1; then
    pkg_mgr="dnf"
  else
    pkg_mgr="yum"
  fi

  # install package from repository
  echo -n "Installing Packages '$package_names' ... "
  pkg_mgr_output=$(sudo $pkg_mgr -y install $package_names 2>&1)
  check_package_manager_response $? $pkg_mgr "$pkg_mgr_output"
}


install_ruby_gem() {
  local gem_name="$1"
  local gem_version="$2"
  local pkg_mgr_name="gem"
  local pkg_mgr_output=""

  local gem_install_cmd="sudo gem install $gem_name"
  if [ ! -z "$gem_version" ]; then
    gem_install_cmd="$gem_install_cmd --version $gem_version"
  else
    gem_version="latest"
  fi

  echo -n "Installing Ruby gem '$gem_name' (version: $gem_version) ... "
  pkg_mgr_output=$($gem_install_cmd 2>&1)
  check_package_manager_response $? $pkg_mgr_name "$pkg_mgr_output"
}


attach_user_group() {
  local groupname=$1
  local username

  # add the group to the user's list of groups
  username=$(id --user --name)
  sudo /usr/sbin/usermod -a -G $groupname $username || return 1
}


set_nofile_limit() {
  local limit_value=$1
  echo "*    hard nofile $limit_value" | sudo tee --append /etc/security/limits.conf > /dev/null
  echo "*    soft nofile $limit_value" | sudo tee --append /etc/security/limits.conf > /dev/null
  echo "root hard nofile $limit_value" | sudo tee --append /etc/security/limits.conf > /dev/null
  echo "root soft nofile $limit_value" | sudo tee --append /etc/security/limits.conf > /dev/null
}


#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#


create_fakes3_config() {
  [ ! -f $FAKE_S3_CONFIG ] || sudo rm -f $FAKE_S3_CONFIG
  sudo tee $FAKE_S3_CONFIG > /dev/null << EOF
CVMFS_S3_HOST=localhost
CVMFS_S3_PORT=$FAKE_S3_PORT
CVMFS_S3_ACCESS_KEY=not
CVMFS_S3_SECRET_KEY=important
CVMFS_S3_BUCKETS_PER_ACCOUNT=1
CVMFS_S3_MAX_NUMBER_OF_PARALLEL_CONNECTIONS=10
CVMFS_S3_BUCKET=$FAKE_S3_BUCKET
EOF
}


start_fakes3() {
  local logfile=$1

  [ ! -d $FAKE_S3_STORAGE ] || sudo rm -fR $FAKE_S3_STORAGE > /dev/null 2>&1 || return 1
  sudo mkdir -p $FAKE_S3_STORAGE                            > /dev/null 2>&1 || return 2
  create_fakes3_config                                      > /dev/null 2>&1 || return 3
  run_background_service $logfile "sudo fakes3 --port $FAKE_S3_PORT --root $FAKE_S3_STORAGE"
}


check_result() {
  local res=$1
  if [ $res -ne 0 ]; then
    echo "Failed!"
  else
    echo "OK"
  fi
}


run_unittests() {
  echo -n "running CernVM-FS unit tests... "
  local xml_output="${UNITTEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX}"
  /usr/bin/cvmfs_unittests --gtest_output="xml:$xml_output" $@ >> $UNITTEST_LOGFILE 2>&1
  local ut_retval=$?
  check_result $ut_retval

  return $ut_retval
}


#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#


die() {
  local msg="$1"
  echo $msg
  exit 103
}
