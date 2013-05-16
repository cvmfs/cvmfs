#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common_test.sh

# run tests
echo "running CernVM-FS test cases..."
cd ${SOURCE_DIRECTORY}/test
./run.sh $TEST_LOGFILE -x src/004-davinci          \
                          src/005-asetup           \
                          src/007-testjobs         \
                          src/016-perl_environment \
                          src/017-dns_timeout      \
                          src/018-dns_injection    \
                          src/019-faulty_proxy     \
                          src/020-server_timeout   \
                          src/023-*                \
                          src/024-*                \
                          src/5*
