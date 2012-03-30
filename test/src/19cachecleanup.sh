
cvmfs_test_name="Cache cleanup"

cvmfs_run_test() {
  logfile=$1

  setup_atlascondb || return 6
  
  cleanup_before=`sudo grep "atlas-condb.cern.ch" /var/log/messages | grep "cleanup cache until" | wc -l`
  find /cvmfs/atlas-condb.cern.ch -type f -size +100M -size -200M | head -n 64 | xargs file > /dev/null || return 1
  cleanup_after=`sudo grep "atlas-condb.cern.ch" /var/log/messages | grep "cleanup cache until" | wc -l` 

  if [ $[$cleanup_after-$cleanup_before] -lt 1 ]; then
    return 2
  fi

  if [ `sudo ls /var/cache/cvmfs2/atlas-condb.cern.ch/quarantaine | wc -l` -ne 0 ]; then
    return 3
  fi
  
  return 0
}

