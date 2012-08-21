
cvmfs_test_name="cvmfs_config chksetup"

cvmfs_run_test() {
  logfile=$1

  setup_atlaslhcb || return 3

  result=`sudo cvmfs_config chksetup`
  if [ "x$result" != "xOK" ]; then
    echo "$result" >> $logfile
    return 4
  fi

  return 0
}

