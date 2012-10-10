
cvmfs_test_name="Dummy test"

cvmfs_run_test() {
  logfile=$1

  echo "Dummy test" >> $logfile || return 1
 
  return 0
}

