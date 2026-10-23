# shellcheck shell=bash
#
# Profile: streamingcache
#
# Runs the CernVM-FS client integration test suite with the streaming cache
# manager enabled globally via /etc/cvmfs/default.d/90-streamingcache.conf.
#
# Derived from test/cloud_testing/platforms/centos9_x86_64-STREAMINGCACHE_test.sh
# and its _setup.sh counterpart.

PROFILE_CLASS_NAME="ClientIntegrationTests"
PROFILE_TESTSUITE="src/0* src/1*"

PROFILE_EXCLUSIONS="\
  src/004-davinci \
  src/005-asetup \
  src/007-testjobs \
  src/011-rmemptyfilesrebuild \
  src/035-unpinumount \
  src/041-rocache \
  src/042-cleanuppipes \
  src/059-fallbackproxy \
  src/081-shrinkwrap \
  src/082-shrinkwrap-cms \
  src/084-premounted \
  src/089-external_cache_plugin \
  src/092-stat \
  src/094-attachmount \
  src/096-cancelreq \
  src/102-reusefd \
  src/103-reloadcachemgr \
  src/112-quota-multiwrite-race"

profile_setup() {
  local conf=/etc/cvmfs/default.d/90-streamingcache.conf
  echo "[profile:streamingcache] writing CVMFS_STREAMING_CACHE=yes to $conf"
  echo "CVMFS_STREAMING_CACHE=yes" | sudo tee "$conf" > /dev/null
}
