#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common.sh

# install RPM packages
echo "installing RPM packages... "
install_rpm "CernVM-FS client" $CLIENT_PACKAGE $KEYS_PACKAGE
install_rpm "CernVM-FS server" $SERVER_PACKAGE $KEYS_PACKAGE

# run tests
echo ""
echo ""
echo ""
echo "running CernVM-FS test cases..."
cd ${SOURCE_DIRECTORY}/test
./run.sh $TEST_LOGFILE
