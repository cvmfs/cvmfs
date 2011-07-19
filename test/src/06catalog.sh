
cvmfs_test_name="Various catalog load scenarios"

do_tests() {
  mkdir -p /tmp/cvmfs_mount || return 3
  
  # root + nested (cold)
  mount -t cvmfs lhcb /tmp/cvmfs_mount >> $logfile 2>&1 || return 5
  ls /tmp/cvmfs_mount/lib/lhcb/LHCB >> $logfile 2>&1 || return 6

  # root + nested + fresh nested (warm)
  sleep 1
  umount /tmp/cvmfs_mount > /dev/null
  mount -t cvmfs lhcb /tmp/cvmfs_mount >> $logfile 2>&1 || return 7
  ls /tmp/cvmfs_mount/lib/lhcb/LHCB >> $logfile 2>&1 || return 8
  ls /tmp/cvmfs_mount/lib/lhcb/DIRAC >> $logfile 2>&1 || return 9

  # updated catalog + updated nested catalog
  sleep 1
  umount /tmp/cvmfs_mount > /dev/null
  cache_dir=`cvmfs_config showconfig lhcb | grep "^CVMFS_CACHE_DIR=" | sed 's/^CVMFS_CACHE_DIR=//'` || return 14
  chksum1=`cat $cache_dir/cvmfs.checksum.lhcb.cern.ch` || return 15
  echo "1$chksum1" > $cache_dir/cvmfs.checksum.lhcb.cern.ch || return 15
  chksum2=`cat $cache_dir/cvmfs.checksum.lhcb.cern.ch-lib-lhcb-LHCB` || return 16
  echo "1$chksum2" > $cache_dir/cvmfs.checksum.lhcb.cern.ch-lib-lhcb-LHCB || return 16
  mount -t cvmfs lhcb /tmp/cvmfs_mount >> $logfile 2>&1 || return 17
  ls /tmp/cvmfs_mount/lib/lhcb/LHCB >> $logfile 2>&1 || return 18
  if [ `head -c 40 $cache_dir/cvmfs.checksum.lhcb.cern.ch` != `echo $chksum1 | head -c40` ]; then
    return 19
  fi
  if [ `head -c 40 $cache_dir/cvmfs.checksum.lhcb.cern.ch-lib-lhcb-LHCB` != `echo $chksum2 | head -c40` ]; then
    return 20
  fi

  # TTL update
  echo "1$chksum1" > $cache_dir/cvmfs.checksum.lhcb.cern.ch || return 22
  sync
  echo 3 > /proc/sys/vm/drop_caches || return 21
  date -s next-week >> $logfile
  stat /tmp/cvmfs_mount >> $logfile 2>&1
  sleep 62
  stat /tmp/cvmfs_mount >> $logfile 2>&1
  RETVAL=$?
  date -s last-week >> $logfile
  [ $RETVAL -eq 0 ] || return 23
  if [ `head -c 40 $cache_dir/cvmfs.checksum.lhcb.cern.ch` != `echo $chksum1 | head -c40` ]; then
    return 24
  fi

  # deep mount
  service cvmfs restartclean >> $logfile 2>&1 || return 10
  mount_cmd=`mount -f -t cvmfs -o deep_mount=/lib/lhcb/LHCB lhcb /tmp/cvmfs_mount | sed 's/\/opt\/lhcb/\/opt\/lhcb\/lib\/lhcb\/LHCB/g' | sed 's/^sg fuse -c //' | sed 's/"//g'`
  echo "$mount_cmd" >> $logfile 2>&1
  ( sg fuse -c "$mount_cmd" ) >> $logfile 2>&1 || return 11
  ls /tmp/cvmfs_mount >> $logfile 2>&1 || return 12
  ls /tmp/cvmfs_mount/LHCB_v32r1 >> $logfile 2>&1 || return 13

  return 0
}

cvmfs_run_test() {
  logfile=$1

  setup_atlaslhcb || return 1
  service cvmfs stop > /dev/null 2>&1 || return 4

  do_tests
  RETVAL=$?

  umount -l /tmp/cvmfs_mount > /dev/null
  rmdir /tmp/cvmfs_mount > /dev/null

  return $RETVAL
}

