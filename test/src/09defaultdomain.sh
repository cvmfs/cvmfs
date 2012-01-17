
cvmfs_test_name="Default domain"

cvmfs_run_test() {
  logfile=$1

  setup_defaultdomain || return 1
  
  ls /cvmfs/atlas.cern.ch >> $logfile || return 1
  ls /cvmfs/lhcb.cern.ch >> $logfile || return 2 
  ls /cvmfs/grid.cern.ch >> $logfile || return 3
  ls /cvmfs/cms.cern.ch >> $logfile || return 4

  service cvmfs flush >> $logfile 2>&1 || return 5
  service cvmfs status >> $logfile 2>&1 || return 6
  service cvmfs restartautofs >> $logfile || return 7
  
  cvmfs_config showconfig grid >> $logfile 2>&1 || return 8
  cvmfs_config showconfig atlas >> $logfile 2>&1 || return 9
  cvmfs_config showconfig cms.cern.ch >> $logfile 2>&1 || return 10
  cvmfs_config chksetup >> $logfile 2>&1 || return 11
  
  return 0
}

