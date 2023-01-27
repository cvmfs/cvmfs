#!/bin/sh

export CVMFS_PLATFORM_NAME="centos8-x86_64_CONTAINER"
export CVMFS_TIMESTAMP=$(date -u +'%Y-%m-%dT%H:%M:%SZ')

# source the common platform independent functionality and option parsing
script_location=$(cd "$(dirname "$0")"; pwd)
. ${script_location}/common_test.sh

retval=0

# Test exclusions:
# 006: Temporarily until a proper k8s is implemented; also update of minikube
#      needed

cd ${SOURCE_DIRECTORY}/test
echo "running CernVM-FS container integration test cases..."
CVMFS_TEST_CLASS_NAME=ContainerIntegrationTests                               \
./run.sh $CLIENT_TEST_LOGFILE -o ${CLIENT_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                              -x container/006-k8s                            \
                                 --                                           \
                                 container/0*                                 \
                              || retval=1


exit $retval
