#!/bin/bash

# source the common platform independent functionality and option parsing
script_location=$(cd "$(dirname "$0")"; pwd)
. ${script_location}/common.sh

CLIENT_PACKAGE=""
SOURCE_DIRECTORY=""
LOG_DIRECTORY=""

while getopts "c:t:l:" option; do
  case $option in
    c)
      CLIENT_PACKAGE=$OPTARG
      ;;
    t)
      SOURCE_DIRECTORY=$OPTARG
      ;;
    l)
      LOG_DIRECTORY=$OPTARG
      ;;
    ?)
      shift $(($OPTIND-2))
      usage "Unrecognized option: $1"
      ;;
  esac
done

if [ "x$CLIENT_PACKAGE"        = "x" ] ||
   [ "x$SOURCE_DIRECTORY"      = "x" ] ||
   [ "x$LOG_DIRECTORY"         = "x" ]; then
  echo "missing parameter(s), cannot run platform dependent test script"
  exit 100
fi

echo -n "Install client package: $CLIENT_PACKAGE ... "
sudo installer -pkg "$CLIENT_PACKAGE" -target / \
    || die "fail (installing CernVM-FS client package)"
echo "done"

env

echo -n "Setting up CernVM-FS environment... "
sudo cvmfs_config setup || die "fail (cvmfs_config setup)"
sudo mkdir -p /var/log/cvmfs-test || die "fail (mkdir /var/log/cvmfs-test)"
sudo chown sftnight /var/log/cvmfs-test || die "fail (chown /var/log/cvmfs-test)"
sudo cvmfs_config chksetup > /dev/null || die "fail (cvmfs_config chksetup)"
echo "done"

echo -n "Installing test dependencies from Homebrew ..."
brew remove sqlite tree cmake jq
brew install sqlite tree cmake jq \
    || die "fail (installing test dependencies from Homebrew)"
echo "done"

echo -n "Increasing open file limits ... "
sudo launchctl limit maxfiles 65536 65536 \
    || die "fail (increasing open file limits)"
echo "done"
