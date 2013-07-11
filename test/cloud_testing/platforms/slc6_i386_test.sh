#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common_test.sh

# run tests
retval=0
run_unittests --gtest_shuffle || retval=$?

echo "running CernVM-FS test cases..."
cd ${SOURCE_DIRECTORY}/test
./run.sh $TEST_LOGFILE -x src/004-davinci              \
                          src/005-asetup               \
                          src/007-testjobs             \
                          src/016-perl_environment     \
                          src/017-dns_timeout          \
                          src/018-dns_injection        \
                          src/019-faulty_proxy         \
                          src/020-server_timeout       \
                          src/024-reload-during-asetup \
                          src/5* || retval=$?

exit $retval
