
cvmfs_test_name="Various catalog load scenarios"

do_tests() {
  mkdir -p /tmp/cvmfs_mount || return 3
  
  # root + nested (cold)
  sudo mount -t cvmfs lhcb /tmp/cvmfs_mount >> $logfile 2>&1 || return 5
  ls /tmp/cvmfs_mount/lib/lhcb/LHCB >> $logfile 2>&1 || return 6

  # root + nested + fresh nested (warm)
  sleep 1
  sudo umount /tmp/cvmfs_mount > /dev/null
  sudo mount -t cvmfs lhcb /tmp/cvmfs_mount >> $logfile 2>&1 || return 7
  ls /tmp/cvmfs_mount/lib/lhcb/LHCB >> $logfile 2>&1 || return 8
  ls /tmp/cvmfs_mount/lib/lhcb/DIRAC >> $logfile 2>&1 || return 9

  # updated catalog + updated nested catalog
  sleep 1
  sudo umount /tmp/cvmfs_mount > /dev/null
  cache_dir=`cvmfs_config showconfig lhcb | grep "^CVMFS_CACHE_DIR=" | sed 's/^CVMFS_CACHE_DIR=//'` || return 14
  chksum1=`sudo cat $cache_dir/cvmfs.checksum.lhcb.cern.ch` || return 15
  sudo sh -c "echo \"1$chksum1\" > $cache_dir/cvmfs.checksum.lhcb.cern.ch" || return 15
  chksum2=`sudo cat $cache_dir/cvmfs.checksum.lhcb.cern.ch-lib-lhcb-LHCB` || return 16
  sudo sh -c "echo \"1$chksum2\" > $cache_dir/cvmfs.checksum.lhcb.cern.ch-lib-lhcb-LHCB" || return 16
  sudo mount -t cvmfs lhcb /tmp/cvmfs_mount >> $logfile 2>&1 || return 17
  ls /tmp/cvmfs_mount/lib/lhcb/LHCB >> $logfile 2>&1 || return 18
  if [ `sudo head -c 40 $cache_dir/cvmfs.checksum.lhcb.cern.ch` != `echo $chksum1 | head -c40` ]; then
    return 19
  fi
  if [ `sudo head -c 40 $cache_dir/cvmfs.checksum.lhcb.cern.ch-lib-lhcb-LHCB` != `echo $chksum2 | head -c40` ]; then
    return 20
  fi

  # TTL update
  sudo sh -c "echo \"1$chksum1\" > $cache_dir/cvmfs.checksum.lhcb.cern.ch" || return 22
  sync
  sudo sh -c "echo 3 > /proc/sys/vm/drop_caches" || return 21
  sudo date -s next-week >> $logfile
  stat /tmp/cvmfs_mount >> $logfile 2>&1
  sleep 70
  stat /tmp/cvmfs_mount >> $logfile 2>&1
  RETVAL=$?
  sudo date -s last-week >> $logfile
  [ $RETVAL -eq 0 ] || return 23
  if [ `sudo head -c 40 $cache_dir/cvmfs.checksum.lhcb.cern.ch` != `echo $chksum1 | head -c40` ]; then
    return 24
  fi

  return 0
}

cvmfs_run_test() {
  logfile=$1

  setup_atlaslhcb || return 1
  sudo /sbin/service cvmfs stop > /dev/null 2>&1 || return 4

  do_tests
  RETVAL=$?

  sudo umount -l /tmp/cvmfs_mount > /dev/null
  sudo rmdir /tmp/cvmfs_mount > /dev/null

  return $RETVAL
}

