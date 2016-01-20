#!/bin/sh

retval=0

#cvmfs_unittests --gtest_shuffle \
#                --gtest_death_test_use_fork || retval=1

logfile="$cvmfs_log_directory/integration_tests.log"

cd "$cvmfs_workspace"


# everything will be placed in the home folder
echo "running CernVM-FS client test cases..."
CVMFS_TEST_CLASS_NAME=ClientIntegrationTests
sudo ./run.sh "$logfile"  src/000-dummy                           \
                          src/001-chksetup                        \
                          src/002-probe                           \
                          src/003-nested                          \
                          src/009-tar                             \
                          src/010-du                              \
                          src/012-ls-l                            \
                          src/013-certificate_cache               \
                          src/014-corrupt_lru                     \
                          src/015-rebuild_on_crash                \
                          src/017-dnstimeout                      \
                          src/018-httpunreachable                 \
                          src/019-httptimeout                     \
                          src/020-emptyrepofailover               \
                          src/021-stacktrace                      \
                          src/022-tacktrace_private_mnt           \
                          src/023-reload_safe_path_traversal      \
                          src/025-proxyfailover                   \
                          src/026-tightcache                      \
                          src/027-automount                       \
                          src/028-negativecache                   \
                          src/029-requeststorm                    \
                          src/030-missingrootcatalog              \
                          src/034-cachecleanup                    \
                          src/035-unpinumount                     \
                          src/036-cacheoverload                   \
                          src/037-strictmount                     \
                          src/038-maxttl                          \
                          src/041-rocache                         \
                          src/042-cleanuppipes                    \
                          src/043-highinodes                      \
                          src/044-unpinonmount                    \
                          src/046-defaultd                        \
                          src/047-blacklist                       \
                          src/048-exportfqrn                      \
                          src/049-cdconf                          \
                          src/050-configrepo                      \
                          src/051-failonbrokenpubkey              \
                          src/053-uuid                            \
                          src/054-geoapi                          \
                          src/058-keysdir                         \
                          src/059-fallbackproxy                   \
                          src/060-hidexattrs                      \
                          src/062-loadtag                         \
                          src/063-uidmap                          \
                          src/064-fsck                            \
                          src/065-http-400                        \
                          src/066-killall                         \
                          src/067-wpad                            \
                          src/068-rocache

retval=$?
exit $retval
