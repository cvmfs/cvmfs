#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common.sh

# install RPM packages
echo "installing RPM packages... "
install_rpm $KEYS_PACKAGE
install_rpm $CLIENT_PACKAGE

# setup environment
echo -n "setting up CernVM-FS environment... "
sudo cvmfs_config setup                          || die "fail (cvmfs_config setup)"
sudo mkdir -p /var/log/cvmfs-test                || die "fail (mkdir /var/log/cvmfs-test)"
sudo chown sftnight:sftnight /var/log/cvmfs-test || die "fail (chown /var/log/cvmfs-test)"
attach_user_group fuse                           || die "fail (add fuse group to user)"
sudo cvmfs_config chksetup                       || die "fail (cvmfs_config chksetup)"

# install test dependencies
echo "installing test dependencies..."
install_from_repo gcc

# run tests
echo "running CernVM-FS test cases..."
cd ${SOURCE_DIRECTORY}/test
./run.sh $TEST_LOGFILE -x src/016-perl_environment \
                          src/017-dns_timeout      \
                          src/018-dns_injection    \
                          src/019-faulty_proxy     \
                          src/020-server_timeout   \
                          src/5*
result=$?

# remove RPM packages
echo "uninstalling RPM packages... "
uninstall_rpm $CLIENT_PACKAGE
uninstall_rpm $KEYS_PACKAGE

# return the test result code
exit $result
