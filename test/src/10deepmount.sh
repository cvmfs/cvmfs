
cvmfs_test_name="Deep mount with non-existing top level directory"

do_tests() {
  mkdir -p /tmp/cvmfs_mount || return 3
  
  # deep mount
  restart_all
  mount_cmd=`sudo mount -f -t cvmfs -o deep_mount=/lib/lhcb/LHCB lhcb /tmp/cvmfs_mount | sed 's/\/opt\/lhcb/\/opt\/lhcb\/lib\/lhcb\/LHCB/' | sed 's/^sg fuse -c //' | sed 's|^/bin/sh -c\(.*\)|\1|' | sed 's/"//g'`
  echo "$mount_cmd" >> $logfile 2>&1
  getent group fuse > /dev/null 2>&1
  if [ $? -eq 0 ]; then
    ( sudo sg fuse -c "$mount_cmd" ) >> $logfile 2>&1 || return 11
  else
    ( sudo sh -c "$mount_cmd" ) >> $logfile 2>&1 || return 11
  fi
  ls /tmp/cvmfs_mount >> $logfile 2>&1 || return 12
  ls /tmp/cvmfs_mount/bla >> $logfile 2>&1
  ls /tmp/cvmfs_mount >> $logfile 2>&1 || return 14

  return 0
}

cvmfs_run_test() {
  logfile=$1

  setup_atlaslhcb || return 1

  do_tests
  RETVAL=$?

  sudo umount -l /tmp/cvmfs_mount > /dev/null
  sudo rmdir /tmp/cvmfs_mount > /dev/null

  return $RETVAL
}

