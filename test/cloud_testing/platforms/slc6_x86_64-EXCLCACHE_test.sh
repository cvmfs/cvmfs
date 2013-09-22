#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common_test.sh

# start apache
echo -n "starting apache... "
sudo service httpd start > /dev/null 2>&1 || die "fail"
echo "OK"

# run tests
retval=0
echo "running CernVM-FS test cases..."
cd ${SOURCE_DIRECTORY}/test
./run.sh $TEST_LOGFILE -x src/004-davinci              \
                          src/005-asetup               \
                          src/007-testjobs             \
                          src/024-reload-during-asetup \
                          src/5* || retval=$?

exit $retval
