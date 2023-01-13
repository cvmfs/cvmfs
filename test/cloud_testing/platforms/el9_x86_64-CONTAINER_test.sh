#!/bin/sh

export CVMFS_PLATFORM_NAME="centos8-x86_64_CONTAINER"
export CVMFS_TIMESTAMP=$(date -u +'%Y-%m-%dT%H:%M:%SZ')

# source the common platform independent functionality and option parsing
script_location=$(cd "$(dirname "$0")"; pwd)
. ${script_location}/common_test.sh

retval=0

cd ${SOURCE_DIRECTORY}/test
echo "running CernVM-FS container integration test cases..."
CVMFS_TEST_CLASS_NAME=ContainerIntegrationTests                               \
./run.sh $CLIENT_TEST_LOGFILE -o ${CLIENT_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                                 container/0*                                 \
                              || retval=1


exit $retval
