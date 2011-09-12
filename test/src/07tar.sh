
cvmfs_test_name="Tar Linux Kernel"

cvmfs_run_test() {
  logfile=$1

  setup_sft || return 1

  rm -f /tmp/cvmfs-atlas-test.tar
  tar cf /tmp/cvmfs-atlas-test.tar /cvmfs/sft.cern.ch/lcg/external/experimental/linux >> $logfile 2>&1 || return 2
  rm -f /tmp/cvmfs-atlas-test.tar
  tar cf /tmp/cvmfs-atlas-test.tar /cvmfs/sft.cern.ch/lcg/external/experimental/linux >> $logfile 2>&1 || return 3
  rm -f /tmp/cvmfs-atlas-test.tar

  check_memory sft 30000 || return 4 

  return 0
}

