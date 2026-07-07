# shellcheck shell=bash
#
# Profile: s3
#
# Runs the CernVM-FS server integration test suite against an S3-compatible
# backend.  CVMFS_TEST_S3_CONFIG, CVMFS_TEST_HTTP_BASE, and
# CVMFS_TEST_S3_STORAGE must be set in the environment before calling run.sh;
# see test/common/container/s3-integration/docker-compose.yml for the
# containerized stack used by .github/workflows/ci_s3_integration.yml.
#
# The exclusion list mirrors the set of tests known to be unsupported or
# flaky on an S3 backend.

PROFILE_CLASS_NAME="S3ServerIntegrationTests"
PROFILE_TESTSUITE="src/5* src/6* src/9*"

PROFILE_EXCLUSIONS="\
  src/514-changechunkedfile \
  src/518-hardlinkstresstest \
  src/519-importlegacyrepo \
  src/522-missingchunkfailover \
  src/523-corruptchunkfailover \
  src/524-corruptmanifestfailover \
  src/525-bigrepo \
  src/528-recreatespoolarea \
  src/530-recreatespoolarea_defaultkey \
  src/533-volatilecacheset \
  src/537-symlinkedbackend \
  src/538-symlinkedstratum1backend \
  src/539-symlinkedvarspoolcvmfs \
  src/542-storagescrubbing \
  src/543-storagescrubbing_scriptable \
  src/550-livemigration \
  src/563-garbagecollectlegacy \
  src/568-migratecorruptrepo \
  src/571-localbackendumask \
  src/572-proxyfailover \
  src/582-autorepairmountpoints \
  src/583-httpredirects \
  src/584-interleavingsnapshot \
  src/585-xattrs \
  src/591-importrepo \
  src/593-nestedwhiteout \
  src/594-backendoverwrite \
  src/595-geoipdbupdate \
  src/598-partialpreload \
  src/599-removehardlinks \
  src/600-securecvmfs \
  src/602-libcvmfs \
  src/605-resurrectancientcatalog \
  src/607-noapache \
  src/608-infofile \
  src/609-metainfofile \
  src/610-altpath \
  src/614-geoservice \
  src/615-externaldata \
  src/616-blacklistconfigrepo \
  src/620-pullmixedrepo \
  src/622-gracefulrmfs \
  src/626-cacheexpiry \
  src/628-pythonwrappedcvmfsserver \
  src/629-reflogrecreation \
  src/630-publish_with_local_cache \
  src/634-reflogchecksum \
  src/638-virtualdir \
  src/643-masterkeycard \
  src/647-bearercvmfs \
  src/661-garbage_collector_statistics \
  src/670-listreflog \
  src/672-publish_stats_hardlinks \
  src/673-acl \
  src/675-statsupload \
  src/682-enter \
  src/684-https_s3 \
  src/686-azureblob_s3 \
  src/687-import_s3 \
  src/691-metalink \
  src/692-https_azureblob_s3 \
  src/693-resetafter \
  src/699-servermount \
  src/702-symlink_caching \
  src/811-commit-gateway \
  src/900-notification_system \
"

profile_setup() {
  local missing=""
  for var in CVMFS_TEST_S3_CONFIG CVMFS_TEST_HTTP_BASE CVMFS_TEST_S3_STORAGE; do
    if [ -z "$(eval "echo \"\${$var}\"")" ]; then
      missing="$missing $var"
    fi
  done
  if [ -n "$missing" ]; then
    echo "[profile:s3] missing required environment variable(s):$missing"
    return 1
  fi
  echo "[profile:s3] CVMFS_TEST_S3_CONFIG=$CVMFS_TEST_S3_CONFIG"
  echo "[profile:s3] CVMFS_TEST_HTTP_BASE=$CVMFS_TEST_HTTP_BASE"
  echo "[profile:s3] CVMFS_TEST_S3_STORAGE=$CVMFS_TEST_S3_STORAGE"
}
