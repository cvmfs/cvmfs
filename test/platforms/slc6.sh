#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common.sh

# install RPM packages
echo "installing RPM packages... "
install_rpm "CernVM-FS keys"   $KEYS_PACKAGE
install_rpm "CernVM-FS client" $CLIENT_PACKAGE
install_rpm "CernVM-FS server" $SERVER_PACKAGE

# setup environment
echo "setting up CernVM-FS environment..."
sudo cvmfs_config setup
sudo cvmfs_config chksetup
sudo httpd

# run tests
echo ""
echo ""
echo ""
echo "running CernVM-FS test cases..."
cd ${SOURCE_DIRECTORY}/test
./run.sh $TEST_LOGFILE
