
cvmfs_test_name="Removing LRU-DB while running"

cvmfs_run_test() {
  logfile=$1

  setup_atlaslhcb || return 3

  sudo [ -f /var/cache/cvmfs2/atlas.cern.ch/cvmfscatalog.cache ] || return 8
  
  ls /cvmfs/atlas.cern.ch >> $logfile || return 1
  cat /cvmfs/atlas.cern.ch/.cvmfsdirtab >> $logfile || return 4
  sudo rm -f /var/cache/cvmfs2/atlas.cern.ch/cvmfscatalog.cache || return 5
  cat /cvmfs/atlas.cern.ch/.cvmfsdirtab >> $logfile || return 6

  setup_atlaslhcb || return 7
  sudo [ -f /var/cache/cvmfs2/atlas.cern.ch/cvmfscatalog.cache ] || return 9

  return 0
}

