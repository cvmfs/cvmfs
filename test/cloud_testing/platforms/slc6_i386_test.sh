#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common_test.sh

ut_retval=0
it_retval=0
mg_retval=0

# running unit test suite
run_unittests --gtest_shuffle || ut_retval=$?

echo "running CernVM-FS test cases..."
cd ${SOURCE_DIRECTORY}/test
./run.sh $TEST_LOGFILE -x src/004-davinci              \
                          src/005-asetup               \
                          src/007-testjobs             \
                          src/024-reload-during-asetup \
                          src/5* || it_retval=$?

echo "running CernVM-FS migration test cases..."
./run.sh $MIGRATIONTEST_LOGFILE migration_tests/001-hotpatch || mg_retval=$?

[ $ut_retval -eq 0 ] && [ $it_retval -eq 0 ] && [ $mg_retval -eq 0 ]
