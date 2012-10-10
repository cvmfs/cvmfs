
cvmfs_test_name="ls -l"

cvmfs_run_test() {
  logfile=$1

  setup_atlaslhcb || return 1

  lserr=`ls -l /cvmfs/atlas.cern.ch 2>&1 1>/dev/null`
  
  if [ "x$lserr" != "x" ]; then
    echo "$lserr" >> $logfile
    return 2
  fi

  return 0
}

