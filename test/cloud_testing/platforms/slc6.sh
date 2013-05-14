#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common.sh

# install RPM packages
echo "installing RPM packages... "
install_rpm $KEYS_PACKAGE
install_rpm $CLIENT_PACKAGE
install_rpm $SERVER_PACKAGE

# setup environment
echo -n "setting up CernVM-FS environment..."
sudo cvmfs_config setup
sudo cvmfs_config chksetup
sudo /usr/sbin/httpd

sudo mkdir -p /var/log/cvmfs-test
sudo chown sftnight:sftnight /var/log/cvmfs-test

# run tests
echo "running CernVM-FS test cases..."
cd ${SOURCE_DIRECTORY}/test
./run.sh $TEST_LOGFILE
result=$?

# remove RPM packages
uninstall_rpm $SERVER_PACKAGE
uninstall_rpm $CLIENT_PACKAGE
uninstall_rpm $KEYS_PACKAGE

# return the test result code
exit $result
