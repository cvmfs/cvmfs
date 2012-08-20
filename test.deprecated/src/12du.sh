
cvmfs_test_name="Recursive listing"

cvmfs_run_test() {
  logfile=$1

  setup_sft || return 1
  
  du -ch --max-depth=3 /cvmfs/sft.cern.ch >> $logfile 2>&1 || return 2
  ps aux | grep cvmfs2 | grep sft.cern.ch >> $logfile
  sudo sh -c "echo 2 > /proc/sys/vm/drop_caches"
  sleep 30
  ps aux | grep cvmfs2 | grep sft.cern.ch >> $logfile
  check_memory sft.cern.ch 60000 || return 3
  
  return 0
}

