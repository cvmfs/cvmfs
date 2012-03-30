
cvmfs_test_name="All Cern repositories"

cvmfs_run_test() {
  logfile=$1

  sudo sh -c "echo \"CVMFS_REPOSITORIES=atlas,atlas-condb,atlas-nightlies,boss,cms,geant4,grid,hone,lcd,lhcb,na61,sft\" > /etc/cvmfs/default.local" || return 1
  sudo sh -c "echo \"CVMFS_SERVER_URL=http://cvmfs-stratum-zero.cern.ch/opt/@org@\" > /etc/cvmfs/domain.d/cern.ch.local"
  sudo sh -c "echo \"CVMFS_HTTP_PROXY=DIRECT\" >> /etc/cvmfs/default.local"
  restart_clean || return 3
  sudo /sbin/service cvmfs probe >> $logfile 2>&1 || return 3

  sudo sh -c "echo \"CVMFS_SERVER_URL=http://cvmfs-stratum-one.cern.ch:8000/opt/@org@\" > /etc/cvmfs/domain.d/cern.ch.local"
  restart_clean || return 30
  sudo /sbin/service cvmfs probe >> $logfile 2>&1 || return 31

  sudo sh -c "echo \"CVMFS_SERVER_URL=http://cernvmfs.gridpp.rl.ac.uk:8000/opt/@org@\" > /etc/cvmfs/domain.d/cern.ch.local"
  restart_clean || return 40
  sudo /sbin/service cvmfs probe >> $logfile 2>&1 || return 41

  sudo sh -c "echo \"CVMFS_SERVER_URL=http://cvmfs.racf.bnl.gov:8000/opt/@org@\" > /etc/cvmfs/domain.d/cern.ch.local"
  restart_clean || return 50
  sudo /sbin/service cvmfs probe >> $logfile 2>&1 || return 51

  sudo rm -f /etc/cvmfs/domain.d/cern.ch.local
  sudo /sbin/service cvmfs restart >> $logfile 2>&1 || return 52

  for r in grid  
  do
    ls /opt/$r >> $logfile 2>&1 || return 4
  done 

  sudo /sbin/service cvmfs stop >> $logfile 2>&1 || return 5
  
  for r in atlas atlas-condb atlas-nightlies boss cms grid hone lcd lhcb na61 sft       
  do
    ls /opt/$r >> $logfile 2>&1
    if [ $? -eq 0 ]; then
      return 6
    fi
  done

  sudo rm -rf /etc/cvmfs/config.d/* || return 7
  sudo rm -rf /etc/cvmfs/local.d/* || return 7

  sudo /sbin/service cvmfs start >> $logfile 2>&1 || return 9
  for r in grid
  do
    ls /opt/$r >> $logfile 2>&1
    if [ $? -eq 0 ]; then
      return 8
    fi
  done
  sudo /sbin/service cvmfs probe >> $logfile 2>&1 || return 10

  return 0
}

