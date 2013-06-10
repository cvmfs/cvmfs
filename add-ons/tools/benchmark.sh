#!/bin/sh

local_repo_name=local_benchmark.cern.ch
riak_repo_name=riak_benchmark.cern.ch

usage() {
  echo "USAGE: $0 <benchmark tarball> <user name> <repo type: local|riak>"
  exit 1
}

if [ $# -ne 3 ]; then
  usage
fi

tarball=$(readlink -f $1)
user_name=$2
repo_type=$3

remove_repo() {
  local repo_name=$1

  if cvmfs_server list | grep -q $repo_name; then
    sudo cvmfs_server rmfs $repo_name -f > /dev/null 2>&1
    return $?
  fi

  return 0
}

create_riak_repo() {
  local repo_name=$1
  local riak_bucket=$2

  echo -n "Create Riak Repositiory... "
  remove_repo $repo_name
  if [ $? -ne 0 ]; then
    echo "failed to remove old"
    return 1
  fi

  sudo cvmfs_server mkfs $repo_name -w http://cernvmbl005/riak/$riak_bucket -u "riak:http://cernvmbl005:8098/riak/$riak_bucket@http://cernvmbl006:8098/riak/$riak_bucket@http://cernvmbl007:8098/riak/$riak_bucket@http://cernvmbl008:8098/riak/$riak_bucket@http://cernvmbl009:8098/riak/$riak_bucket" -o $user_name > /dev/null 2>&1
  if [ $? -ne 0 ]; then
    echo "fail"
    return 1
  fi
  echo "done"

  return 0
}

create_local_repo() {
  local repo_name=$1

  echo -n "Create Local Repository... "
  remove_repo $repo_name
  if [ $? -ne 0 ]; then
    echo "failed to remove old"
    return 1
  fi

  sudo cvmfs_server mkfs $repo_name -o $user_name > /dev/null 2>&1
  if [ $? -ne 0 ]; then
    echo "fail"
    return 1
  fi
  echo "done"

  return 0
}

fill_repo() {
  local repo_name=$1
  local tar_archive=$2

  cvmfs_server transaction $repo_name

  echo -n "Fill Repository... "
  cd /cvmfs/$repo_name
  tar -zxf $tar_archive > /dev/null 2>&1
  if [ $? -ne 0 ]; then
    echo "fail"
    return 1
  fi
  cd - > /dev/null
  echo "done"

  return 0
}

publish_repo() {
  local repo_name=$1

  echo -n "Publish Repository... "
  time cvmfs_server publish $repo_name
}

# main()

repository=""

case $repo_type in
  local)
    repository=$local_repo_name
    create_local_repo $repository || exit 1
    ;;
  riak)
    repository=$riak_repo_name
    create_riak_repo $repository cvmfs || exit 1
    ;;
  *)
    usage
esac

fill_repo $repository $tarball || exit 1
publish_repo $repository || exit 1

