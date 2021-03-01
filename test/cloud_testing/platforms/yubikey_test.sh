#!/bin/sh

export CVMFS_PLATFORM_NAME="centos7-x86_64-YUBIKEY"
export CVMFS_TIMESTAMP=$(date -u +'%Y-%m-%dT%H:%M:%SZ')

# source the common platform independent functionality and option parsing
script_location=$(cd "$(dirname "$0")"; pwd)
. ${script_location}/common_test.sh

retval=0

cd ${SOURCE_DIRECTORY}/test
echo "running CernVM-FS masterkeycard test case..."
CVMFS_TEST_CLASS_NAME=MasterkeycardTest                                       \
./run.sh $SERVER_TEST_LOGFILE -o ${SERVER_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                                 src/643-masterkeycard                        \
                              || retval=1

exit $retval
