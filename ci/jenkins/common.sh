#!/bin/sh

make_build_tag() {
  cvmfs_build_tag="cvmfs-???"
  case $BUILD_TYPE in
    nightly|incremental)
      cvmfs_build_tag="cvmfs-$CVMFS_SNAPSHOT_VERSION"
      ;;
    release)
      cvmfs_build_tag="cvmfs-$CVMFS_VERSION"
      ;;
    *)
      echo "FAIL: unknown build type '$BUILD_TYPE'"
      exit 1
      ;;
  esac

  echo "$cvmfs_build_tag"
}

# figure out some information about the version to be built
CVMFS_GIT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
CVMFS_COMMIT="$(git rev-parse HEAD)"
CVMFS_VERSION="$(grep CVMFS_VERSION CMakeLists.txt | cut -d' ' -f3)"
CVMFS_SNAPSHOT_VERSION="git-$(echo $CVMFS_COMMIT | head -c16)"
CVMFS_BUILD_DIR="build"
CVMFS_BUILD_RESULTS="build_results"
CVMFS_BUILD_TAG="$(make_build_tag)"

CVMFS_INIT_SCRIPTS_VERSION="1.0.18-2"
CVMFS_KEYS_VERSION="1.4-1"

echo
echo "Jenkins Environment"
echo "~~~~~~~~~~~~~~~~~~~"
echo "Job Name:              $JOB_NAME"
echo "Building:              $BUILD_NUMBER"
echo "Timestamp:             $(date)"
echo "Build Node:            $NODE_NAME"
echo "Labels:                $NODE_LABELS"
echo "workspace:             $WORKSPACE"
echo "Build URL:             $BUILD_URL"
echo "Job URL:               $JOB_URL"
echo "Working Dir:           $(pwd)"
echo ""
echo "System"
echo "~~~~~~"
echo "System (uname -srn):   $(uname -srn)"
echo "User:                  $(whoami)"
echo
echo "CVMFS Build Environment"
echo "~~~~~~~~~~~~~~~~~~~~~~~"
echo "CVMFS Version:           $CVMFS_VERSION"
echo "Snapshot Build Version:  $CVMFS_SNAPSHOT_VERSION"
echo "Build Results:           $CVMFS_BUILD_RESULTS"
echo "Package Type:            $PACKAGE_TYPE"
echo "Build Type:              $BUILD_TYPE"
echo "Git Branch:              $CVMFS_GIT_BRANCH"
echo "Git Commit:              $CVMFS_COMMIT"
echo "Last Change:             $(git log -1 --pretty=%B)"
echo

