
cvmfs_test_name="Caching of certificate"

cvmfs_run_test() {
  logfile=$1
  
  setup_atlaslhcb || return 1
  ls /cvmfs/lhcb.cern.ch >> $logfile || return 2  
  sleep 1
  sudo umount /cvmfs/lhcb.cern.ch >> $logfile 2>&1 || return 3
  sleep 1
  sudo rm -f /var/cache/cvmfs2/lhcb.cern.ch/cvmfs.checksum.lhcb.cern.ch
  ls /cvmfs/lhcb.cern.ch >> $logfile || return 4
  
  sudo cvmfs-talk -i lhcb.cern.ch sqlite memory | grep "certificate disk cache hits/misses 1/0" >> $logfile
  if [ $? -ne 0 ]; then
    return 5
  fi

  return 0 
}

