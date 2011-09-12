
cvmfs_test_name="Nested catalogs with same prefix"

cvmfs_run_test() {
  logfile=$1

  setup_atlaslhcb || return 3
  
  stat /cvmfs/lhcb.cern.ch/lib/lhcb/GEANT4/GEANT4_v92r4/Makefile >> $logfile || return 1
  stat /cvmfs/lhcb.cern.ch/lib/lhcb/GEANT4/GEANT4_v92r4p1/Makefile >> $logfile || return 2
  return 0
}

