#!/bin/sh

#
# This script wraps the unit test run of the CernVM-FS python library.
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/common.sh

usage() {
  echo "Usage: $0 [<XML output location>]"
  echo "This script runs the CernVM-FS python library unit tests"
  exit 1
}

if [ $# -gt 1 ]; then
  usage
fi
PY_UT_XML_PREFIX=$1

# check if python is capable of running the unit tests on the current platform
which python > /dev/null 2>&1                    || complain "no python available"
compare_versions "$(python_version)" -gt "2.6.0" || complain "ancient python version"
check_python_module "xmlrunner"                  || complain "no python-xmlrunner installed"
check_python_module "dateutil"                   || complain "no python-dateutil installed"
check_python_module "requests"                   || complain "no python-requests installed"

# run the unit tests
UNITTESTS="${SCRIPT_LOCATION}/../python/cvmfs/test"
if [ ! -z "$PY_UT_XML_PREFIX" ]; then
  echo "running python unit tests (XML output to $PY_UT_XML_PREFIX)..."
  python $UNITTESTS --xml-prefix "$PY_UT_XML_PREFIX"
else
  echo "running python unit tests..."
  python $UNITTESTS
fi
