#!/bin/sh

export LC_ALL=C

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


die() {
  local msg="$1"
  echo $msg
  exit 103
}
