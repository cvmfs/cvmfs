#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common_test.sh

ut_retval=0
it_retval=0
mg_retval=0

# check $PATH variable
extend_path() {
  local path_entry=$1
  if ! echo "$PATH" | grep -q $path_entry; then
    export PATH="$PATH:$path_entry"
  fi
}
extend_path "/usr/sbin"
extend_path "/sbin"

# running unit test suite
run_unittests --gtest_shuffle || ut_retval=$?

echo "running CernVM-FS test cases..."
cd ${SOURCE_DIRECTORY}/test
./run.sh $TEST_LOGFILE -x src/518-hardlinkstresstest   \
                          src/523-corruptchunkfailover \
                          src/524-corruptmanifestfailover || it_retval=$?

echo "running CernVM-FS migration test cases..."
./run.sh $MIGRATIONTEST_LOGFILE migration_tests/001-hotpatch || mg_retval=$?

[ $ut_retval -eq 0 ] && [ $it_retval -eq 0 ] && [ $mg_retval -eq 0 ]
