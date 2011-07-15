
cvmfs_test_name="Nested catalogs with same prefix"

cvmfs_run_test() {
  logfile=$1

  setup_atlaslhcb || return 3
  
  stat /cvmfs/lhcb.cern.ch/lib/lhcb/PANORAMIX/PANORAMIX_v18r1/Makefile >> $logfile || return 1
  stat /cvmfs/lhcb.cern.ch/lib/lhcb/PANORAMIX/PANORAMIX_v18r10/Makefile >> $logfile || return 2
  return 0
}

