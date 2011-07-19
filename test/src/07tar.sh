
cvmfs_test_name="Tar Linux Kernel"

cvmfs_run_test() {
  logfile=$1

  setup_atlaslhcb || return 1

  rm -f /tmp/cvmfs-atlas-test.tar
  tar cf /tmp/cvmfs-atlas-test.tar /cvmfs/atlas.cern.ch/test >> $logfile 2>&1 || return 2
  rm -f /tmp/cvmfs-atlas-test.tar
  tar cf /tmp/cvmfs-atlas-test.tar /cvmfs/atlas.cern.ch/test >> $logfile 2>&1 || return 3
  rm -f /tmp/cvmfs-atlas-test.tar

  check_memory atlas 30000 || return 4 

  return 0
}

