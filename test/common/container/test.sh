#!/bin/bash

cd "/home/sftnight/cvmfs/test"

TEST_LOG_DIRECTORY=/tmp
CLIENT_TEST_LOGFILE=/tmp/cvmfs-client-test.log
SERVER_TEST_LOGFILE=/tmp/cvmfs-server-test.log
                                     
# echo "running CernVM-FS client test cases..."
# ./run.sh $CLIENT_TEST_LOGFILE -o ${SERVER_TEST_LOGFILE} -s "quick"            \
#                               -x src/005-asetup                               \
#                                  src/004-davinci                              \
#                                  src/007-testjobs                             \
#                                  src/017-dnstimeout                           \
#                                  src/019-httptimeout                          \
#                                  src/020-emptyrepofailover                    \
#                                  src/030-missingrootcatalog                   \
#                                  src/056-lowspeedlimit                        \
#                                  src/065-http-400                             \
#                                  src/066-killall                              \
#                                  src/084-premounted                           \
#                                  src/095-fuser                                \
#                                  src/096-cancelreq                            \
#                                  --                                           \
#                                  src/0*                                       \
#                                  src/1*                                       \
#                               || exit 1
# 

echo "running CernVM-FS server test cases..."
CVMFS_TEST_UNIONFS=overlayfs                                                  \
./run.sh $SERVER_TEST_LOGFILE -o ${SERVER_TEST_LOGFILE} -s "quick"            \
                              -x src/500-mkrepo                               \
                                 src/518-hardlinkstresstest                   \
                                 src/525-bigrepo                              \
                                 src/572-proxyfailover                        \
                                 src/577-garbagecollecthiddenstratum1revision \
                                 src/578-garbagecollecthiddenstratum1revision \
                                 src/579-garbagecollecthiddenstratum1revision \
                                 src/580-garbagecollecthiddenstratum1revision \
                                 src/595-geoipdbupdate                        \
                                 src/600-securecvmfs                          \
                                 src/607-noapache                             \
                                 src/615-externaldata                         \
                                 src/616-blacklistconfigrepo                  \
                                 src/620-pullmixedrepo                        \
                                 src/624-chunkedexternalgraft                 \
                                 src/627-reflog                               \
                                 src/628-pythonwrappedcvmfsserver             \
                                 src/672-publish_stats_hardlinks              \
                                 src/673-acl                                  \
                                 src/684-https_s3                             \
                                 src/686-azureblob_s3                         \
                                 src/687-import_s3                            \
                                 src/702-symlink_caching                      \
                                 --                                           \
                                 src/5*                                       \
                                 src/6*                                       \
                                 src/7*                                       \
                              || exit 1

