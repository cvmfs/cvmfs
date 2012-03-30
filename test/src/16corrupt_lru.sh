
cvmfs_test_name="Recover from corrupted LRU DB"

cvmfs_run_test() {
  logfile=$1

  setup_atlaslhcb || return 1
  ls /cvmfs/atlas.cern.ch >> $logfile 2>&1 || return 2

  numBefore=`sudo grep "LRU database corrupted" /var/log/messages | grep cvmfs2 | wc -l`

  sudo umount /cvmfs/atlas.cern.ch >> $logfile 2>&1 || return 3
  sudo sh -c "echo bla > /var/cache/cvmfs2/atlas.cern.ch/cvmfscatalog.cache" || return 4
  
  ls /cvmfs/atlas.cern.ch >> $logfile 2>&1 || return 5   
  
  sync
  numAfter=`sudo grep "LRU database corrupted" /var/log/messages | grep cvmfs2 | wc -l`

  if [ $[$numAfter-$numBefore] -ne 1 ]; then
    return 6
  fi

  return 0
}

