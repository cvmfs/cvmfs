#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(cd "$(dirname "$0")"; pwd)
. ${script_location}/common_test.sh

retval=0

cd ${SOURCE_DIRECTORY}/test
echo "running CernVM-FS client test cases..."
CVMFS_TEST_CLASS_NAME=ClientIntegrationTests                                  \
./run.sh $CLIENT_TEST_LOGFILE -o ${CLIENT_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                              -x src/005-asetup                               \
                                 src/004-davinci                              \
                                 src/007-testjobs                             \
                                 src/011-rmemptyfilesrebuild                  \
                                 src/014-corrupt_lru                          \
                                 src/015-rebuild_on_crash                     \
                                 src/024-reload-during-asetup                 \
                                 src/040-aliencache                           \
                                 src/041-rocache                              \
                                 src/042-cleanuppipes                         \
                                 src/044-unpinonmount                         \
                                 src/057-parallelmakecache                    \
                                 src/068-rocache                              \
                                 src/070-tieredcache                          \
                                 --                                           \
                                 src/0*                                       \
                              || retval=1

# ToDo: not yet possible because there are no external cache managers for previous versions
#echo "running CernVM-FS migration test cases..."
#CVMFS_TEST_CLASS_NAME=MigrationTests                                              \
#./run.sh $MIGRATIONTEST_LOGFILE -o ${MIGRATIONTEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
#                                   migration_tests/*                              \
#                                || retval=1

exit $retval
