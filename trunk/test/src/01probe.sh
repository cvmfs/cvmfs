
cvmfs_test_name="Probing atlas, lhcb"

cvmfs_run_test() {
  logfile=$1

  setup_atlaslhcb || return 3
  
  ls /cvmfs/atlas.cern.ch >> $logfile || return 1
  ls /cvmfs/lhcb.cern.ch >> $logfile || return 2 
  return 0
}

