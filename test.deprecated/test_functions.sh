
TEST_CVMFS_PROXY=${TEST_CVMFS_PROXY:=http://ca-proxy.cern.ch:3128}

restart_clean() {
  sudo /sbin/service cvmfs start > /dev/null 2>&1  || return 1  # fc 16 hack
  sudo /sbin/service cvmfs restartclean > /dev/null 2>&1  || return 1

  return 0
}

restart_all() {
  restart_clean
  sudo /sbin/service autofs stop > /dev/null || return 1
  sudo killall -9 automount > /dev/null 2>&1 # fc16 hack
  sudo /sbin/service autofs start > /dev/null || return 1

  return 0
}

setup_atlaslhcb() {
  sudo sh -c 'echo "CVMFS_REPOSITORIES=atlas,lhcb" > /etc/cvmfs/default.local' || return 1
  sudo sh -c "echo \"CVMFS_HTTP_PROXY=${TEST_CVMFS_PROXY}\" >> /etc/cvmfs/default.local" || return 1
  #sudo sh -c 'echo "CVMFS_DEBUGLOG=/tmp/cvmfs_test.log" >> /etc/cvmfs/default.local' || return 1
  restart_all || return 2
  sudo /sbin/service cvmfs probe > /dev/null 2>&1 || return 3

  return 0
}

setup_atlascondb() {
  sudo sh -c "echo \"CVMFS_REPOSITORIES=atlas-condb\" > /etc/cvmfs/default.local" || return 1
  sudo sh -c "echo \"CVMFS_HTTP_PROXY=${TEST_CVMFS_PROXY}\" >> /etc/cvmfs/default.local" || return 1
  sudo sh -c "echo \"CVMFS_QUOTA_LIMIT=1000\" >>  /etc/cvmfs/default.local" || return 1
  restart_all
  sudo /sbin/service cvmfs probe > /dev/null 2>&1 || return 3

  return 0
}

setup_sft() {
  sudo sh -c "echo \"CVMFS_REPOSITORIES=sft\" > /etc/cvmfs/default.local" || return 1
  sudo sh -c "echo \"CVMFS_HTTP_PROXY=${TEST_CVMFS_PROXY}\" >> /etc/cvmfs/default.local" || return 1
  #sudo sh -c "echo \"CVMFS_DEBUGLOG=/tmp/cvmfs_test.log\" >> /etc/cvmfs/default.local" || return 1
  restart_all
  sudo /sbin/service cvmfs probe > /dev/null 2>&1 || return 3

  return 0
}


setup_defaultdomain() {
  sudo sh -c "echo \"CVMFS_REPOSITORIES=atlas.cern.ch,lhcb,cms.cern.ch,grid\" > /etc/cvmfs/default.local" || return 1
  sudo sh -c "echo \"CVMFS_HTTP_PROXY=${TEST_CVMFS_PROXY}\" >> /etc/cvmfs/default.local" || return 1
  sudo sh -c "echo \"CVMFS_QUOTA_LIMIT=1000\" >>  /etc/cvmfs/default.local" || return 1
  #sudo sh -c "echo \"CVMFS_DEBUGLOG=/tmp/cvmfs_test.log\" >> /etc/cvmfs/default.local" || return 1
  restart_all
  sudo /sbin/service cvmfs probe > /dev/null 2>&1 || return 3

  return 0
}


extract_local_repo() {
  repo=$1
  sudo rm -rf server/pub || return 1
  cd server || return 2
  tar xfz ${repo}.tar.gz || return 3
  #sudo chown -R `whoami` server/pub
  cd ..

  return 0
}


resign_local() {
  for c in `find server/pub/catalogs -name .cvmfspublished`
  do
    cp $c $c.unsigned
    server/cvmfs_sign-linux32 -c /tmp/cvmfs_test.crt -k /tmp/cvmfs_test.key -n 127.0.0.1 $c >> $logfile 2>&1 || return 10
  done
  cp /tmp/whitelist.test.signed server/pub/catalogs/.cvmfswhitelist >> $logfile 2>&1
}

setup_local() {
  #rm -rf /var/cache/cvmfs2/127.0.0.1
  if [ ! -x server/mongoose/mongoose ]; then
    #sudo rm -rf server/mongoose
    cd server
    tar xvfz mongoose-2.11.tgz >> $logfile 2>&1 || return 1
    patch mongoose/mongoose.c < mongoose.cvmfs.patch >> $logfile 2>&1 || return 1
    patch mongoose/Makefile < Makefile.cvmfs.patch >> $logfile 2>&1 || return 1
    cd mongoose
    make linux >> $logfile 2>&1 || return 2
    cd ../..
  fi
  screen -dmS webserver sh -c "server/mongoose/mongoose -r server/pub -p 8080 >> $logfile 2>&1"
  screen -dmS webserver2 sh -c "server/mongoose/mongoose-timeout -r server/pub -p 8081 >> $logfile 2>&1"
  if [ -z "$1" ]; then
    screen -dmS proxy sh -c "server/faulty_proxy.pl 3128 all >> $logfile 2>&1"
  else
    screen -dmS proxy sh -c "server/faulty_proxy.pl 3128 $1  >> $logfile 2>&1"
  fi
  screen -dmS proxy2 sh -c "server/faulty_proxy.pl 3129 none http://localhost:8080  >> $logfile 2>&1"
  screen -dmS proxy3 sh -c "server/faulty_proxy.pl 3130 none http://localhost:8081  >> $logfile 2>&1"
  sleep 1

  sudo sh -c "echo \"CVMFS_REPOSITORIES=127.0.0.1\" > /etc/cvmfs/default.local" || return 3
  sudo sh -c "echo \"CVMFS_TIMEOUT=1\" >> /etc/cvmfs/default.local" || return 3
  sudo sh -c "echo \"CVMFS_TIMEOUT_DIRECT=1\" >> /etc/cvmfs/default.local" || return 3
  sudo sh -c "echo \"CVMFS_QUOTA_LIMIT=8000\" >> /etc/cvmfs/default.local" || return 3
  #sudo sh -c "echo \"CVMFS_DEBUGLOG=/tmp/cvmfs_test.log\" >> /etc/cvmfs/default.local" || return 3
  if [ -z "$1" ]; then
    sudo sh -c "echo \"CVMFS_SERVER_URL=http://127.0.0.1:8080/catalogs\" > /etc/cvmfs/config.d/127.0.0.1.conf" || return 6
  else
    sudo sh -c "echo \"CVMFS_SERVER_URL=http://127.0.0.1:8081/catalogs,http://127.0.0.1:8080/catalogs\" > /etc/cvmfs/config.d/127.0.0.1.conf" || return 6
  fi
  sudo sh -c "echo \"CVMFS_PUBLIC_KEY=/tmp/cvmfs_master.pub\" >> /etc/cvmfs/config.d/127.0.0.1.conf" || return 6
  if [ -z "$1" ]; then
    sudo sh -c "echo 'CVMFS_HTTP_PROXY=\"http://127.0.0.1:3128\"' >> /etc/cvmfs/config.d/127.0.0.1.conf" || return 6
  else
    sudo sh -c "echo 'CVMFS_HTTP_PROXY=\"DIRECT\"' >> /etc/cvmfs/config.d/127.0.0.1.conf" || return 6
  fi

  openssl genrsa -out /tmp/cvmfs_test.key 2048 >> $logfile 2>&1 || return 7
  openssl req -new -subj "/C=CH/ST=n\/a/L=Geneva/O=CERN/OU=PH-SFT/CN=CVMFS Test Certificate" -key /tmp/cvmfs_test.key -out /tmp/cvmfs_test.csr >> $logfile 2>&1 || return 8
  openssl x509 -req -days 365 -in /tmp/cvmfs_test.csr -signkey /tmp/cvmfs_test.key -out /tmp/cvmfs_test.crt >> $logfile 2>&1 || return 9
  resign_local
  openssl x509 -fingerprint -sha1 -in /tmp/cvmfs_test.crt | grep "SHA1 Fingerprint" | sed 's/SHA1 Fingerprint=//' > /tmp/whitelist.test.unsigned || return 11
  echo `date -u "+%Y%m%d%H%M%S"` > /tmp/whitelist.test.signed || return 11
  echo "E`date -u --date='next month' "+%Y%m%d%H%M%S"`" >> /tmp/whitelist.test.signed || return 11
  echo "N127.0.0.1" >> /tmp/whitelist.test.signed || return 11
  cat /tmp/whitelist.test.unsigned >> /tmp/whitelist.test.signed || return 11
  sha1=`openssl sha1 < /tmp/whitelist.test.signed | tr -d " " | sed 's/(stdin)=//' | head -c40` || return 11
  echo "--" >> /tmp/whitelist.test.signed || return 11
  echo $sha1 >> /tmp/whitelist.test.signed || return 11
  echo $sha1 | head -c 40 > /tmp/whitelist.test.sha1 || return 11
  openssl genrsa -out /tmp/cvmfs_master.key 2048 >> $logfile 2>&1 || return 12
  openssl rsa -in /tmp/cvmfs_master.key -pubout -out /tmp/cvmfs_master.pub >> $logfile 2>&1 || return 12
  openssl rsautl -inkey /tmp/cvmfs_master.key -sign -in /tmp/whitelist.test.sha1 -out /tmp/whitelist.test.signature >> $logfile 2>&1 || return 12
  cat /tmp/whitelist.test.signature >> /tmp/whitelist.test.signed || return 12
  cp /tmp/whitelist.test.signed server/pub/catalogs/.cvmfswhitelist || return 13
  dd if=/dev/zero of=/tmp/cvmfs.faulty bs=1024 count=8 >> $logfile 2>&1 || return 14
  echo "faulty file" >> /tmp/cvmfs.faulty || return 14

  return 0
}

cleanup_local() {
  sudo rm -f /tmp/cvmfs_test.key /tmp/cvmfs_test.csr /tmp/cvmfs_test.crt /etc/cvmfs/config.d/127.0.0.1.conf /tmp/whitelist.test.* /tmp/cvmfs_master.key /tmp/cvmfs_master.pub /tmp/cvmfs.faulty
  sudo killall -w screen >> $logfile 2>&1
  restart_all
}

check_memory() {
  instance=$1
  limit=$2

  pid=`attr -qg pid /cvmfs/$instance` || return 1
  rss=`sudo cat /proc/$pid/status | grep VmRSS | awk '{print $2}'` || return 2

  if [ -f /sys/kernel/mm/transparent_hugepage/enabled ]; then
    grep -q "\[never\]" /sys/kernel/mm/transparent_hugepage/enabled
    if [ $? -ne 0 ]; then
      limit=$[$limit*2]
    fi
  fi
  if [ -f /sys/kernel/mm/redhat_transparent_hugepage/enabled ]; then
    grep -q "\[never\]" /sys/kernel/mm/redhat_transparent_hugepage/enabled
    if [ $? -ne 0 ]; then
      limit=$[$limit*2]
    fi
  fi

  if [ $rss -gt $limit ]; then
    echo "Memory limit exceeded" >&2
    return 3
  fi

  return 0
}

check_time() {
  start_time=$1
  end_time=$2
  limit=$3

  diff_time=$[$end_time-$start_time]

  if [ $diff_time -gt $limit ]; then
    echo "Time limit exceeded" >&2
    return 1
  fi

  return 0
}

