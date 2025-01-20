#!/bin/bash

cd "/home/sftnight/cvmfs/test"

TEST_LOG_DIRECTORY=/tmp
CLIENT_TEST_LOGFILE=/tmp/cvmfs-client-test.log
SERVER_TEST_LOGFILE=/tmp/cvmfs-server-test.log

echo "running CernVM-FS client test cases..."
./run.sh $CLIENT_TEST_LOGFILE -s "quick"                                      \
                              -x src/104-concurrent_mounts                    \
                                 src/105-streaming-cache                      \
                                 src/059-fallbackproxy                        \
                                 --                                           \
                                 src/0*                                       \
                                 src/1*                                       \
                              || exit 1


echo "running CernVM-FS server test cases..."
CVMFS_TEST_UNIONFS=overlayfs                                                  \
./run.sh $SERVER_TEST_LOGFILE -s "quick"                                      \
                              -x src/514-changechunkedfile                    \
                                 src/518-hardlinkstresstest                   \
                                 src/522-missingchunkfailover                 \
                                 src/523-corruptchunkfailover                 \
                                 src/524-corruptmanifestfailover              \
                                 src/525-bigrepo                              \
                                 src/564-stratum1keysdir                      \
                                 src/572-proxyfailover                        \
                                 src/577-garbagecollecthiddenstratum1revision \
                                 src/578-automaticgarbagecollection           \
                                 src/579-garbagecollectstratum1legacytag      \
                                 src/580-automaticgarbagecollectionstratum1   \
                                 src/593-nestedwhiteout                       \
                                 src/595-geoipdbupdate                        \
                                 src/600-securecvmfs                          \
                                 src/608-infofile                             \
                                 src/610-altpath                              \
                                 src/615-externaldata                         \
                                 src/616-blacklistconfigrepo                  \
                                 src/618-repometainfo                         \
                                 src/620-pullmixedrepo                        \
                                 src/621-snapshotallgcall                     \
                                 src/624-chunkedexternalgraft                 \
                                 src/626-cacheexpiry                          \
                                 src/627-reflog                               \
                                 src/628-pythonwrappedcvmfsserver             \
                                 src/634-reflogchecksum                       \
                                 src/638-virtualdir                           \
                                 src/647-bearercvmfs                          \
                                 src/652-tarball_ingest_various_paths         \
                                 src/655-tarball_multiple_ingest_same_location\
                                 src/656-tarball_remove_directory             \
                                 src/666-ingestownership                      \
                                 src/667-ingestnested                         \
                                 src/671-stats_db_upgrade                     \
                                 src/672-publish_stats_hardlinks              \
                                 src/682-enter                                \
                                 src/684-https_s3                             \
                                 src/686-azureblob_s3                         \
                                 src/687-import_s3                            \
                                 src/688-checkall                             \
                                 src/699-servermount                          \
                                 src/700-overlayfs_validation                 \
                                 src/702-symlink_caching                      \
                                 --                                           \
                                 src/5*                                       \
                                 src/6*                                       \
                                 src/7*                                       \
                              || exit 1

