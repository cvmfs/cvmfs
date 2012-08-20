
cvmfs_test_name="mount race"

cvmfs_run_test() {
  logfile=$1

  setup_atlaslhcb || return 1

  mkdir -p /tmp/cvmfstest >> $logfile 2>&1
  sudo umount /tmp/cvmfstest >> $logfile 2>&1
  sleep 1
  sudo cvmfs2 -o pubkey=/etc/cvmfs/keys/cern.ch.pub /tmp/cvmfstest http://cvmfs-stratum-one.cern.ch/opt/lhcb >> $logfile 2>&1 || return 2

  pid=`sudo cvmfs_talk -c /var/lib/cvmfs -i default pid`
  sudo cvmfs2 -o pubkey=/etc/cvmfs/keys/cern.ch.pub /tmp/cvmfstest http://cvmfs-stratum-one.cern.ch/opt/lhcb >> $logfile 2>&1 &
  if [ $? -ne 0 ]; then
    return 3
  fi

  pid2=`sudo cvmfs_talk -c /var/lib/cvmfs -i default pid`
  if [ $pid -ne $pid2 ]; then
    sudo umount /tmp/cvmfstest >> $logfile
    return 4
  fi
  sudo kill $pid
  sleep 2
  pid2=`sudo cvmfs_talk -c /var/lib/cvmfs -i default pid`
  if [ $pid -eq $pid2 ]; then
    sudo umount /tmp/cvmfstest >> $logfile
    return 4
  fi

  sudo umount /tmp/cvmfstest >> $logfile
  rm -rf /tmp/cvmfstest

  return 0
}

