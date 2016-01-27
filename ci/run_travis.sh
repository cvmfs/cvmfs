#!/bin/bash

report_error() {
  log_file="$1"
  FAILURES=$(( FAILURES+1 ))
  echo "Failed!"
  echo ""
  echo ""
  cat "$log_file"
  echo ""
  echo ""
}

check_failure() {
  code=$1
  if [ $code -eq 0 ]; then
    echo "FAILED"
  else
    echo "PASSED"
  fi
}

CPPLINT_FAILED=1
BUILD_FAILED=1
UNITTESTS_FAILED=1
FAILURES=0

# create the build directory
mkdir -p build

# run the cpplint first
echo -n "RUNNING CPPLINT ......................................................................................... "
ci/run_cpplint.sh > cpplint.log 2>&1                                      || CPPLINT_FAILED=0 report_error "$(pwd)/cpplint.log"
echo "Done"

# Build CVMFS (make -j stresses the memory of travis)
echo -n "BUILDING CernVM-FS ...................................................................................... "
cd build
cmake -DBUILD_UNITTESTS=yes -DBUILD_PRELOADER=yes .. > build.log 2>&1     || BUILD_FAILED=1 report_error "$(pwd)/build.log"
make >> build.log 2>&1                                                    || BUILD_FAILED=1 report_error "$(pwd)/build.log"
echo "Done"

#running the unit tests on mac fails because travis osx machines have limited resources
echo -n "RUNNING UNIT TESTS ...................................................................................... "
if [[ "$TRAVIS_OS_NAME" == "linux" ]]; then
  test/unittests/cvmfs_unittests --gtest_shuffle --gtest_filter="-*Slow:T_Dns.CaresResolverLocalhost:T_Dns.NormalResolverCombined:T_Dns.CaresResolverMany" > unittests.log 2>&1  || UNITTESTS_FAILED=1 report_error "$(pwd)/unittests.log"
  echo "Done"
else
  echo "Skipped"
fi

################################################################################
if [ $FAILURES -ne 0 ]; then
  echo ""
  echo ""
  echo "SUMMARY"
  echo "-------"
  echo -n "CPPLINT ------------ "
  check_failure $CPPLINT_FAILED
  echo -n "BUILD -------------- "
  check_failure $BUILD_FAILED
  if [[ "$TRAVIS_OS_NAME" == "linux" ]]; then
    echo -n "UNITTESTS ---------- "
    check_failure $UNITTESTS_FAILED
  fi

  echo ""
  echo ""
  echo "Please check the corresponding logs and fix the errors"
fi


