
cvmfs_test_name="All Cern repositories"

cvmfs_run_test() {
  logfile=$1

  echo "CVMFS_REPOSITORIES=alice,atlas,atlas-condb,atlas-nightlies,boss,cms,geant4,grid,hone,lcd,lhcb,na61,sft" > /etc/cvmfs/default.local || return 1
  echo "CVMFS_SERVER_URL=http://cvmfs-stratum-zero.cern.ch/opt/@org@" > /etc/cvmfs/domain.d/cern.ch.local
  echo "CVMFS_HTTP_PROXY=DIRECT" >> /etc/cvmfs/default.local
  service cvmfs restartclean >> $logfile 2>&1 || return 2
  service cvmfs probe >> $logfile 2>&1 || return 3

  echo "CVMFS_SERVER_URL=http://cernvm-bkp.cern.ch/opt/@org@" > /etc/cvmfs/domain.d/cern.ch.local
  service cvmfs restartclean >> $logfile 2>&1 || return 20
  service cvmfs probe >> $logfile 2>&1 || return 21

  echo "CVMFS_SERVER_URL=http://cvmfs-stratum-one.cern.ch:8000/opt/@org@" > /etc/cvmfs/domain.d/cern.ch.local
  service cvmfs restartclean >> $logfile 2>&1 || return 30
  service cvmfs probe >> $logfile 2>&1 || return 31

  echo "CVMFS_SERVER_URL=http://cernvmfs.gridpp.rl.ac.uk:8000/opt/@org@" > /etc/cvmfs/domain.d/cern.ch.local
  service cvmfs restartclean >> $logfile 2>&1 || return 40
  service cvmfs probe >> $logfile 2>&1 || return 41

  echo "CVMFS_SERVER_URL=http://cvmfs.racf.bnl.gov:8000/opt/@org@" > /etc/cvmfs/domain.d/cern.ch.local
  service cvmfs restartclean >> $logfile 2>&1 || return 50
  service cvmfs probe >> $logfile 2>&1 || return 51

  rm -f /etc/cvmfs/domain.d/cern.ch.local
  service cvmfs restart >> $logfile 2>&1 || return 52

  for r in grid hone lcd 
  do
    ls /opt/$r >> $logfile 2>&1 || return 4
  done 

  service cvmfs stop >> $logfile 2>&1 || return 5
  
  for r in atlas atlas-condb atlas-nightlies boss cms grid hone lcd lhcb na61 sft       
  do
    ls /opt/$r >> $logfile 2>&1
    if [ $? -eq 0 ]; then
      return 6
    fi
  done

  rm -rf /etc/cvmfs/config.d/* || return 7
  rm -rf /etc/cvmfs/local.d/* || return 7

  service cvmfs start >> $logfile 2>&1 || return 9
  for r in atlas-condb grid hepsoft hone lcd 
  do
    ls /opt/$r >> $logfile 2>&1
    if [ $? -eq 0 ]; then
      return 8
    fi
  done
  service cvmfs probe >> $logfile 2>&1 || return 10

  return 0
}

