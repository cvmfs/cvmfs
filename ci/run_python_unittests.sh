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
check_python_module "ctypes"                     || complain "no python-ctypes installed"
check_python_module "M2Crypto"                   || complain "no m2crypto installed"
if compare_versions "$(python_version)" -lt "2.7.0"; then
  # unittest2 is a back-port of the Python 2.7 unittest features
  check_python_module "unittest2" || complain "python 2.6 but no unittest2 installed"
fi

# run the unit tests
UNITTEST_RUNNER="${SCRIPT_LOCATION}/../python/run_unittests.py"
if [ ! -z "$PY_UT_XML_PREFIX" ]; then
  echo "running python unit tests (XML output to $PY_UT_XML_PREFIX)..."
  python $UNITTEST_RUNNER --xml-prefix "$PY_UT_XML_PREFIX"
else
  echo "running python unit tests..."
  python $UNITTEST_RUNNER
fi
