
cvmfs_test_name="Setup Davinci"

cvmfs_run_test() {
  logfile=$1

  setup_atlaslhcb || return 10
  
  . /cvmfs/lhcb.cern.ch/etc/login.sh >> $logfile 2>&1 || return 1
  ( time SetupProject.sh Davinci ) >> $logfile 2>&1 || return 2
  sync
  sudo sh -c "echo 3 > /proc/sys/vm/drop_caches" || return 3
  
  start_time=`date -u +%s` 
  ( time SetupProject.sh Davinci ) >> $logfile 2>&1 || return 4
  end_time=`date -u +%s`
  check_time $start_time $end_time 180 || return 9 

  all_catalogs=`sudo cvmfs-talk -i lhcb open catalogs | awk '{print $1}' | sort` || return 5
  uniq_catalogs=`echo "$all_catalogs" | uniq` || return 6

  if [ "$all_catalogs" != "$uniq_catalogs" ]; then
    echo "Catalog mismatch!" >&2
    echo "All catalogs: $all_catalogs" >> $logfile
    echo "Unique catalogs: $uniq_catalogs" >> $logfile
    return 7
  fi
  echo "All catalogs: $all_catalogs" >> $logfile
  sudo cvmfs-talk -i lhcb sqlite memory >> $logfile

  check_memory lhcb.cern.ch 50000 || return 8
 
  return 0
}

