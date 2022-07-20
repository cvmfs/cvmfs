#!/bin/bash

#
# This script wraps the unit test run of CernVM-FS.
#

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/common.sh

usage() {
  echo "Usage: $0 [-q only quick tests] [-s shrinkwrap test binary]\\"
  echo "          [-c cache plugin binary] [-g GeoAPI sources] \\"
  echo "          [-d run the ducc unittests] \\"
  echo "          [-G run the gateway unittests (deprecated, always run)] \\"
  echo "          [-p run the publish unit tests] \\"
  echo "          <unittests binary> <XML output location>"
  echo "This script runs the CernVM-FS unit tests"
  exit 1
}

CVMFS_UNITTESTS_QUICK=0
CVMFS_SHRINKWRAP_TEST_BINARY="$CVMFS_SHRINKWRAP_TEST_BINARY"
CVMFS_CACHE_PLUGIN=
CVMFS_GEOAPI_SOURCES=
CVMFS_TEST_PUBLISH=0

RETVAL=0

while getopts "qc:g:s:l:Gdp" option; do
  case $option in
    q)
      CVMFS_UNITTESTS_QUICK=1
    ;;
    c)
      CVMFS_CACHE_PLUGIN=$OPTARG
    ;;
    g)
      CVMFS_GEOAPI_SOURCES=$OPTARG
    ;;
    s)
      CVMFS_SHRINKWRAP_TEST_BINARY=$OPTARG
    ;;
    l)
      # Preloading a library now unused
      :
    ;;
    d)
      # Deprecated; container tools unit tests are always run when the code can build
    ;;
    p)
      CVMFS_TEST_PUBLISH=1
    ;;
    G)
      # Deprecated; Gateway unit tests are now always run when the gateway code can be built
    ;;
    ?)
      usage
    ;;
  esac
done

shift $(( $OPTIND - 1 ))
if [ $# -lt 2 ]; then
  usage
fi

CVMFS_UNITTESTS_BINARY=$1
CVMFS_UNITTESTS_RESULT_LOCATION=$2

# check if only a quick subset of the unittests should be run
test_filter='-'
if [ $CVMFS_UNITTESTS_QUICK = 1 ]; then
  echo "*** Running only quick tests (without suffix 'Slow')"
  test_filter='-*Slow'
fi

# run the Python unittests for the GeoAPI
if [ "x$CVMFS_GEOAPI_SOURCES" != "x" ]; then
  pushd $CVMFS_GEOAPI_SOURCES
  # python2 is not available on MacOS
  PYTHON_COMMAND="python"
  if ! python -V >/dev/null 2>&1; then
    PYTHON_COMMAND="python2"
    if ! python2 -V >/dev/null 2>&1; then
      PYTHON_COMMAND="python3"
    fi
  fi
  echo "*** Running Geo-API unit tests"
  $PYTHON_COMMAND test_cvmfs_geo.py
  RETVAL=$(( RETVAL | $? ))
  popd
else
  echo "*** Skipping Geo-API unit tests"
fi

# run the cache plugin unittests
if [ "x$CVMFS_CACHE_PLUGIN" != "x" ]; then
  CVMFS_CACHE_UNITTESTS="$(dirname $CVMFS_UNITTESTS_BINARY)/cvmfs_test_cache"
  CVMFS_CACHE_LOCATOR_PORT=4224
  CVMFS_CACHE_CONFIG=$(mktemp /tmp/cvmfs-unittests-XXXXX)
  echo "CVMFS_CACHE_PLUGIN_SIZE=1000" > $CVMFS_CACHE_CONFIG
  echo "CVMFS_CACHE_PLUGIN_TEST=yes" >> $CVMFS_CACHE_CONFIG
  echo "CVMFS_CACHE_DIR=/tmp/cvmfs_cache_test_dir/" >> $CVMFS_CACHE_CONFIG
  i=0
  for plugin in $(echo $CVMFS_CACHE_PLUGIN | tr : " "); do
    if [ -x $plugin ]; then
      # Use a different port for every plugin
      CVMFS_CACHE_LOCATOR_PORT=$((CVMFS_CACHE_LOCATOR_PORT + i))
      i=$((i + 1))
      CVMFS_CACHE_LOCATOR=tcp=127.0.0.1:$CVMFS_CACHE_LOCATOR_PORT
      echo "CVMFS_CACHE_PLUGIN_LOCATOR=$CVMFS_CACHE_LOCATOR" >> $CVMFS_CACHE_CONFIG
      echo -n "*** Running cache plugin unit tests for cache plugin $plugin on $CVMFS_CACHE_LOCATOR"
      echo " (with XML output ${CVMFS_UNITTESTS_RESULT_LOCATION}.$(basename $plugin))"
      # All our plugins take a configuration file as a parameter
      plugin_pid="$($plugin $CVMFS_CACHE_CONFIG)"
      echo "cache plugin started as PID $plugin_pid"
      $CVMFS_CACHE_UNITTESTS $CVMFS_CACHE_LOCATOR \
        --gtest_output=xml:${CVMFS_UNITTESTS_RESULT_LOCATION}.$(basename $plugin)
      RETVAL=$(( RETVAL | $? ))
      /bin/kill $plugin_pid
      rm -rf "/tmp/cvmfs_cache_test_dir/"
    else
      echo "Warning: plugin $plugin not found, skipping"
    fi
  done
  rm -f $CVMFS_CACHE_CONFIG
fi

if [ "x$CVMFS_SHRINKWRAP_TEST_BINARY" != "x" ]; then
  echo "*** Running shrinkwrap unit tests (with XML output ${CVMFS_UNITTESTS_RESULT_LOCATION}.shrinkwrap)..."
  $CVMFS_SHRINKWRAP_TEST_BINARY --gtest_shuffle                                     \
    --gtest_output=xml:${CVMFS_UNITTESTS_RESULT_LOCATION}.shrinkwrap \
    --gtest_filter=$test_filter
  RETVAL=$(( RETVAL | $? ))
else
  echo "*** Skipping shrinkwrap unit tests"
fi

if can_build_gateway; then
  echo "*** Running gateway unit tests (with XML output ${CVMFS_UNITTESTS_RESULT_LOCATION}.gateway)"
  pushd ${SCRIPT_LOCATION}/../gateway > /dev/null
  CVMFS_TEST_GATEWAY_OUTPUT=$(mktemp /tmp/cvmfs-gateway-unittests-XXXXX)
  go test -v -mod=vendor ./... > $CVMFS_TEST_GATEWAY_OUTPUT 2>&1
  RETVAL=$(( RETVAL | $? ))
  cat $CVMFS_TEST_GATEWAY_OUTPUT | go-junit-report > ${CVMFS_UNITTESTS_RESULT_LOCATION}.gateway
  rm -f $CVMFS_TEST_GATEWAY_OUTPUT
  popd > /dev/null
else
  echo "*** Skipping gateway unit tests"
fi

if can_build_ducc; then
  echo "*** Running container tools unit tests (with XML output ${CVMFS_UNITTESTS_RESULT_LOCATION}.ducc)"
  pushd ${SCRIPT_LOCATION}/../ducc > /dev/null
  CVMFS_TEST_DUCC_OUTPUT=$(mktemp /tmp/cvmfs-ducc-unittests-XXXXX)
  go test -v -mod=vendor ./... > $CVMFS_TEST_DUCC_OUTPUT 2>&1
  RETVAL=$(( RETVAL | $? ))
  cat $CVMFS_TEST_DUCC_OUTPUT | go-junit-report > ${CVMFS_UNITTESTS_RESULT_LOCATION}.ducc
  rm -f $CVMFS_TEST_DUCC_OUTPUT
  popd > /dev/null
else
  echo "*** Skipping container tools unit tests"
fi

if [ $CVMFS_TEST_PUBLISH = 1 ]; then
  echo "*** Running publish unit tests (with XML output $CVMFS_UNITTESTS_RESULT_LOCATION)"
  CVMFS_PUBLISH_UNITTESTS="$(dirname $CVMFS_UNITTESTS_BINARY)/cvmfs_test_publish"
  $CVMFS_PUBLISH_UNITTESTS --gtest_shuffle                                     \
                           --gtest_output=xml:$CVMFS_UNITTESTS_RESULT_LOCATION \
                           --gtest_filter=$test_filter
  RETVAL=$(( RETVAL | $? ))
else
  echo "*** Skipping publish unit tests"
fi

# run the unit tests
echo "*** Running core unit tests (with XML output $CVMFS_UNITTESTS_RESULT_LOCATION)..."
$CVMFS_UNITTESTS_BINARY --gtest_shuffle                                     \
                        --gtest_output=xml:$CVMFS_UNITTESTS_RESULT_LOCATION \
                        --gtest_filter=$test_filter
RETVAL=$(( RETVAL | $? ))
exit $RETVAL
