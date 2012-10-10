
cvmfs_test_name="Fail-over proxy and host"

cvmfs_run_test() {
  logfile=$1

  #setup_local /catalogs/data/4a/dontfail36ef343ea3d2ee62fa41a6eecbaf1d44206adbC || return 1
  extract_local_repo pub
  setup_local none || return 1  
  restart_all
  RETVAL=$?

  ls /cvmfs/127.0.0.1 >> $logfile 2>&1
  RETVAL=$[$RETVAL+$?]

  cleanup_local

  setup_local none || return 1
  restart_all
  RETVAL=$[$RETVAL+$?]
  sudo sh -c "echo 'CVMFS_HTTP_PROXY=\"http://127.0.0.1:3130;http://127.0.0.1:3129\"' >> /etc/cvmfs/config.d/127.0.0.1.conf"  
  sudo sh -c "echo 'CVMFS_SERVER_URL=\"http://127.0.0.1:8081/catalogs\"' >> /etc/cvmfs/config.d/127.0.0.1.conf"
  
  ls /cvmfs/127.0.0.1 >> $logfile 2>&1
  RETVAL=$[$RETVAL+$?]

  cleanup_local  
  
  return $RETVAL
}

