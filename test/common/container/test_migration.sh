#!/bin/bash

cd "/home/sftnight/cvmfs/test"

MIGRATION_TEST_LOGFILE=/tmp/cvmfs-migration-test.log

echo "running CernVM-FS hotpatch/migration tests..."
export CVMFS_PACKAGE_DIR="/tmp"
CVMFS_TEST_CLASS_NAME=MigrationTests ./run.sh $MIGRATION_TEST_LOGFILE         \
                              migration_tests/001*                            \
                              migration_tests/500*
