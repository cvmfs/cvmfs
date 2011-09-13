
cvmfs_test_name="Setup ATLAS"

cvmfs_run_test() {
  logfile=$1

  setup_atlaslhcb || return 6
  
  export ATL_LOCAL_ROOT=/cvmfs/atlas.cern.ch/repo
  export ATLAS_LOCAL_ROOT_BASE=$ATL_LOCAL_ROOT/ATLASLocalRootBase
  echo "asetup cold cache" >> $logfile
  . ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh >> $logfile 2>&1 || return 1
  $AtlasSetup/python/asetup.py --debugprint 17.0.0 >> $logfile 2>&1 || return 2

  echo "asetup warm cache" >> $logfile
  start_time=`date -u +%s`
  $AtlasSetup/python/asetup.py --debugprint 17.0.0 >> $logfile 2>&1 || return 3 
  end_time=`date -u +%s`
  echo "$[$end_time-$start_time] seconds required" >> $logfile

  check_time $start_time $end_time 20 || return 4
  check_memory atlas 30000 || return 5
 
  return 0
}

