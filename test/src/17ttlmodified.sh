
cvmfs_test_name="Catch modified file after catalog reload"

do_tests() {
  logfile=$1

  extract_local_repo ttlbase || return 1
  setup_local none || return 1
  service cvmfs restartclean >> $logfile 2>&1 || return 1

  ls /cvmfs/127.0.0.1 >> $logfile 2>&1 || return 2
  file=`cat /cvmfs/127.0.0.1/file` || return 3
  if [ $file != "vanilla" ]; then
    return 4
  fi  
  
  extract_local_repo ttlmodified || return 5
  resign_local
  date -s next-week >> $logfile || return 6
  stat /cvmfs/127.0.0.1 >> $logfile
  sleep 62
  file=`cat /cvmfs/127.0.0.1/file`
  date -s last-week >> $logfile || return 7
  if [ $file != "modified" ]; then
    return 8
  fi

  return 0
}


cvmfs_run_test() {
  logfile=$1

  do_tests $logfile
  RETVAL=$?

  cleanup_local

  return $RETVAL
}

