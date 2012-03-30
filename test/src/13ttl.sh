
cvmfs_test_name="Reload on TTL expiry / remount"

do_tests() {
  logfile=$1

  extract_local_repo ttl || return 1
  setup_local none || return 1
  restart_clean || return 1

  ls /cvmfs/127.0.0.1 >> $logfile 2>&1 || return 2
  ls /cvmfs/127.0.0.1/dir2/bla >> $logfile 2>&1 || return 2
  ls /cvmfs/127.0.0.1//dir1/dir1-1/bla >> $logfile 2>&1 
  if [ $? -eq 0 ]; then
    return 2
  fi
  stat /cvmfs/127.0.0.1/dir1/dir1-1/dir1-1-1/bla >> $logfile 2>&1
  if [ $? -eq 0 ]; then
    return 2
  fi

  # Everything loaded OK
  tree /cvmfs/127.0.0.1 >> $logfile 2>&1 || return 3
  num_dirty=`sudo cvmfs-talk -i 127.0.0.1 open catalogs | grep ! | wc -l`
  if [ $num_dirty -ne 0 ]; then
    return 4
  fi
  num_all=`sudo cvmfs-talk -i 127.0.0.1 open catalogs | wc -l`
  if [ $num_all -ne 6 ]; then
    return 8
  fi
  main_cat=`sudo cvmfs-talk -i 127.0.0.1 open catalogs | head -2 | tail -1 | cut -d\| -f2` 
  main_expiry=`sudo cvmfs-talk -i 127.0.0.1 open catalogs | head -2 | tail -1 | cut -d\| -f3`

  # New TTL on Expiry
  sudo date -s next-week >> $logfile || return 23
  stat /cvmfs/127.0.0.1 >> $logfile  || return 25
  sleep 62
  stat /cvmfs/127.0.0.1 >> $logfile || return 26
  new_expiry=`sudo cvmfs-talk -i 127.0.0.1 open catalogs | head -2 | tail -1 | cut -d\| -f3`
  sudo date -s last-week >> $logfile || return 27 
  if [ "$new_expiry" == "$main_expiry" ]; then
    return 28
  fi

  # New TTL on Crap load
  restart_clean || return 29
  tree /cvmfs/127.0.0.1 >> $logfile 2>&1 || return 30  
  nested_expiry=`sudo cvmfs-talk -i 127.0.0.1 open catalogs | tail -1 | cut -d\| -f3`
  sudo cvmfs-talk -i 127.0.0.1 host set http://foo >> $logfile || return 31
  sudo date -s next-week >> $logfile || return 32
  stat /cvmfs/127.0.0.1 >> $logfile  || return 33
  new_expiry=`sudo cvmfs-talk -i 127.0.0.1 open catalogs | tail -1 | cut -d\| -f3`
  sudo date -s last-week >> $logfile || return 32
  if [ "$nested_expiry" == "$main_expiry" ]; then
    return 34
  fi

  # Reload on TTL
  restart_clean || return 35
  tree /cvmfs/127.0.0.1 >> $logfile 2>&1 || return 36
  nested_expiry=`sudo cvmfs-talk -i 127.0.0.1 open catalogs | tail -1 | cut -d\| -f3`
  extract_local_repo ttl-new || return 37
  resign_local
  sudo date -s next-week >> $logfile || return 38
  stat /cvmfs/127.0.0.1 >> $logfile || return 40
  sudo cvmfs-talk -i 127.0.0.1 remount | grep -qi already 
  concurrent_remount=$?
  sleep 62
  stat /cvmfs/127.0.0.1 >> $logfile  
  sudo date -s last-week >> $logfile || return 39  
  if [ $concurrent_remount -ne 0 ]; then
    return 41
  fi
  num_dirty=`sudo cvmfs-talk -i 127.0.0.1 open catalogs | grep ! | wc -l`
  new_expiry=`sudo cvmfs-talk -i 127.0.0.1 open catalogs | tail -1 | cut -d\| -f3`
  new_cat=`sudo cvmfs-talk -i 127.0.0.1 open catalogs | head -2 | tail -1 | cut -d\| -f2`
  if [ $num_dirty -ne 3 ]; then
    return 42
  fi
  if [ "$nested_expiry" == "$main_expiry" ]; then
    return 43
  fi
  if [ "$main_cat" == "$new_cat" ]; then
    return 45
  fi
  
  # Prepare freshly for remount tests
  extract_local_repo ttl || return 44
  resign_local
  restart_clean || return 44
  ls /cvmfs/127.0.0.1 >> $logfile 2>&1 || return 44
  ls /cvmfs/127.0.0.1/dir2/bla >> $logfile 2>&1 || return 44
  ls /cvmfs/127.0.0.1//dir1/dir1-1/bla >> $logfile 2>&1
  if [ $? -eq 0 ]; then
    return 44
  fi
  stat /cvmfs/127.0.0.1/dir1/dir1-1/dir1-1-1/bla >> $logfile 2>&1
  if [ $? -eq 0 ]; then
    return 44
  fi
  tree /cvmfs/127.0.0.1 >> $logfile 2>&1 || return 44
  num_dirty=`sudo cvmfs-talk -i 127.0.0.1 open catalogs | grep ! | wc -l`
  if [ $num_dirty -ne 0 ]; then
    return 44
  fi
  num_all=`sudo cvmfs-talk -i 127.0.0.1 open catalogs | wc -l`
  if [ $num_all -ne 6 ]; then
    return 44
  fi
  main_cat=`sudo cvmfs-talk -i 127.0.0.1 open catalogs | head -2 | tail -1 | cut -d\| -f2` 

  # Reload crap, stay local
  extract_local_repo ttl-new || return 5
  ls /cvmfs/127.0.0.1 >> $logfile 2>&1 || return 6
  main_cat_new=`sudo cvmfs-talk -i 127.0.0.1 open catalogs | head -2 | tail -1 | cut -d\| -f2`
  echo "$main_cat" >> $logfile
  echo "$main_cat_new" >> $logfile
  sudo cvmfs-talk -i 127.0.0.1 open catalogs >> $logfile 2>&1
  if [ "$main_cat" != "$main_cat_new" ]; then
    return 7
  fi  

  # Reload on changed catalog
  resign_local
  sudo cvmfs-talk -i 127.0.0.1 remount >> $logfile 2>&1 || return 9
  sleep 2
  ls /cvmfs/127.0.0.1 >> $logfile 2>&1 || return 9
  main_cat_new=`sudo cvmfs-talk -i 127.0.0.1 open catalogs | head -2 | tail -1 | cut -d\| -f2`
  echo "$main_cat" >> $logfile
  echo "$main_cat_new" >> $logfile
  if [ "$main_cat" == "$main_cat_new" ]; then
    return 10
  fi
  
  # Check for dirty catalogs
  sudo cvmfs-talk -i 127.0.0.1 open catalogs >> $logfile 2>&1
  num_dirty=`sudo cvmfs-talk -i 127.0.0.1 open catalogs | grep ! | wc -l`
  if [ $num_dirty -ne 3 ]; then
    return 11
  fi

  # Hit a valid one
  ls /cvmfs/127.0.0.1/dir3 >> $logfile 2>&1 || return 12
  num_dirty=`sudo cvmfs-talk -i 127.0.0.1 open catalogs | grep ! | wc -l`
  if [ $num_dirty -ne 3 ]; then
    return 13
  fi

  # Hit a dirty one
  ls /cvmfs/127.0.0.1/dir2/bla >> $logfile 2>&1 || return 14
  num_dirty=`sudo cvmfs-talk -i 127.0.0.1 open catalogs | grep ! | wc -l`
  sudo cvmfs-talk -i 127.0.0.1 open catalogs >> $logfile 2>&1
  if [ $num_dirty -ne 2 ]; then
    return 15
  fi

  # Remount and hit a dirty one
  restart_clean || return 16
  ls /cvmfs/127.0.0.1 >> $logfile 2>&1 || return 17
  stat /cvmfs/127.0.0.1/dir3 >> $logfile 2>&1 || return 18
  stat /cvmfs/127.0.0.1/dir1/dir1-1/dir1-1-1/bla >> $logfile 2>&1 || return 19  

  # Remount fail
  sudo cvmfs-talk -i 127.0.0.1 proxy set DIRECT >> $logfile 2>&1 || return 20
  sudo cvmfs-talk -i 127.0.0.1 host set http://foo >> $logfile 2>&1 || return 21
  sudo cvmfs-talk -i 127.0.0.1 remount | grep -q Fail
  if [ $? -ne 0 ]; then
    return 22
  fi

  return 0
}


cvmfs_run_test() {
  logfile=$1

  do_tests $logfile
  RETVAL=$?

  cleanup_local

  return $RETVAL
}

