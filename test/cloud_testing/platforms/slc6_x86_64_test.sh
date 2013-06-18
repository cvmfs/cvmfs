#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common_test.sh

# start apache
sudo service httpd start

# run tests
echo "running CernVM-FS unit tests..."
cvmfs_unittests --gtest_shuffle \
                --gtest_death_test_use_fork >> $UNITTEST_LOGFILE 2>&1 || die "fail"

echo "running CernVM-FS test cases..."
cd ${SOURCE_DIRECTORY}/test
./run.sh $TEST_LOGFILE -x src/004-davinci              \
                          src/005-asetup               \
                          src/007-testjobs             \
                          src/024-reload-during-asetup \
                          src/518-hardlinkstresstest
