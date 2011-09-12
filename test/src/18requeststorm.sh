
cvmfs_test_name="No request storm on local I/O errors"

do_tests() {
  logfile=$1

  extract_local_repo ttlbase || return 1
  setup_local none || return 1
  service cvmfs restartclean >> $logfile 2>&1 || return 1

  ls /cvmfs/127.0.0.1 >> $logfile 2>&1 || return 2
  rm -rf /var/cache/cvmfs2/127.0.0.1/7a
  
  numBefore=`grep -i "failed to open" /var/log/messages | grep cvmfs2 | wc -l`

  cat /cvmfs/127.0.0.1/file > /dev/null 2>>$logfile
  if [ $? -eq 0 ]; then
    return 3
  fi

  sync
  numAfter=`grep -i "failed to open" /var/log/messages | grep cvmfs2 | wc -l`
  
  if [ $[$numAfter-$numBefore] -ne 1 ]; then
    return 4
  fi

  attempts=0
  start_time=`date -u +%s`
  while [ $[`date -u +%s`-$start_time] -lt 10 ]
  do 
    cat /cvmfs/127.0.0.1/file >/dev/null 2>&1
    attempts=$[$attempts+1]
  done
  echo $attempts >> $logfile

  if [ $attempts -gt 16 ]; then
    return 5
  fi

  mkdir /var/cache/cvmfs2/127.0.0.1/7a
  chown cvmfs:cvmfs /var/cache/cvmfs2/127.0.0.1/7a
  cat /cvmfs/127.0.0.1/file >> $logfile
  if [ $? -ne 0 ]; then
    return 6
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

