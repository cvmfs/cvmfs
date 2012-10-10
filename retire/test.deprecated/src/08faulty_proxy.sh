
cvmfs_test_name="Faulty proxy"

cvmfs_run_test() {
  logfile=$1

  extract_local_repo pub
  setup_local || return 1
  RETVAL=$? 
 
  ls /cvmfs/127.0.0.1 >> $logfile 2>&1
  RETVAL=$[$RETVAL+$?]

  cat /cvmfs/127.0.0.1/bla >> $logfile 2>&1
  RETVAL=$[$RETVAL+$?]
  
  cat /cvmfs/127.0.0.1/blubb >> $logfile 2>&1
  RETVAL=$[$RETVAL+$?]

  ls /cvmfs/127.0.0.1/nested >> $logfile 2>&1
  RETVAL=$[$RETVAL+$?]

  cleanup_local
  
  return $RETVAL
}

