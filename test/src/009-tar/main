
cvmfs_test_name="Tar Linux Kernel"

cvmfs_run_test() {
  logfile=$1

  setup_sft || return 1
  
  tmpdir=/tmp
  if [ "x$CVMFS_TEMP_DIR" != "x" ]; then
    tmpdir=$CVMFS_TEMP_DIR
  fi 

  rm -f $tmpdir/cvmfs-atlas-test.tar
  tar cf $tmpdir/cvmfs-atlas-test.tar /cvmfs/sft.cern.ch/lcg/external/experimental/linux >> $logfile 2>&1 || return 2
  rm -f $tmpdir/cvmfs-atlas-test.tar
  tar cf $tmpdir/cvmfs-atlas-test.tar /cvmfs/sft.cern.ch/lcg/external/experimental/linux >> $logfile 2>&1 || return 3
  rm -f $tmpdir/cvmfs-atlas-test.tar

  check_memory sft.cern.ch 50000 || return 4 

  sudo rm -rf /var/cache/cvmfs2/sft.cern.ch/* >> $logfile 2>&1

  return 0
}

