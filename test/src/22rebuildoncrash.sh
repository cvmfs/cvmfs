
cvmfs_test_name="Rebuild cache db after crash"

cvmfs_run_test() {
  logfile=$1
  
  setup_atlaslhcb || return 1
  
  ls /cvmfs/lhcb.cern.ch >> $logfile || return 2  
  sudo ls /var/cache/cvmfs2/lhcb.cern.ch/running >> $logfile || return 3
  sleep 1
  sudo umount /cvmfs/lhcb.cern.ch >> $logfile 2>&1 || return 4
  sleep 1
  sudo ls /var/cache/cvmfs2/lhcb.cern.ch/running >> $logfile 2>&1
  if [ $? -eq 0 ]; then
    return 5
  fi

  ls /cvmfs/lhcb.cern.ch >> $logfile || return 6
  ls /cvmfs/lhcb.cern.ch >> $logfile || return 7
  sudo cvmfs-talk -i lhcb.cern.ch cache list | grep "unkown (automatic rebuild)" >> $logfile
  if [ $? -eq 0 ]; then
    return 9
  fi
  pid=`sudo cvmfs-talk -i lhcb.cern.ch pid`
  sudo kill -9 $pid || return 7
  sudo umount /cvmfs/lhcb.cern.ch
  sleep 1
  ls /cvmfs/lhcb.cern.ch >> $logfile || return 7
  sudo cvmfs-talk -i lhcb.cern.ch cache list | grep "unknown (automatic rebuild)" >> $logfile
  if [ $? -ne 0 ]; then
    return 8
  fi

  return 0 
}

