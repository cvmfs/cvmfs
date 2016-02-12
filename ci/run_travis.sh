#!/bin/bash


report_error() {
  local log_file="$1"

  FAILURES=$(( FAILURES+1 ))
  echo "Failed!"
  echo ""
  echo ""
  cat "$log_file"
  echo ""
  echo ""
}

running_on_linux() {
  [[ "$TRAVIS_OS_NAME" == "linux" ]]
}

check_failure() {
  local result=$1

  if $result ; then
    echo "FAILED"
  else
    echo "PASSED"
  fi
}

CPPLINT_FAILED=false
BUILD_FAILED=false
UNITTESTS_FAILED=false
FAILURES=0


echo ""
echo ""
# run the cpplint first
echo -n "RUNNING CPPLINT ......................................................................................... "
ci/run_cpplint.sh > cpplint.log 2>&1                                      || { CPPLINT_FAILED=true; report_error "$(pwd)/cpplint.log"; }
if ! $CPPLINT_FAILED ; then
  echo "Done"
fi

# Build CVMFS (make -j stresses the memory of travis machines)
echo -n "BUILDING CernVM-FS ...................................................................................... "
mkdir -p build && cd build
cmake -DBUILD_UNITTESTS=yes -DBUILD_PRELOADER=yes .. > build.log 2>&1  && \
make >> "$(pwd)/build.log" 2>&1                                           || { BUILD_FAILED=true; report_error "$(pwd)/build.log"; }
if ! $BUILD_FAILED ; then
  echo "Done"
fi

#running the unit tests on mac fails because travis osx machines have limited resources
echo -n "RUNNING UNIT TESTS ...................................................................................... "
if running_on_linux; then
  test/unittests/cvmfs_unittests --gtest_shuffle --gtest_filter="-*Slow:T_Dns.CaresResolverLocalhost:T_Dns.NormalResolverCombined:T_Dns.CaresResolverMany" > unittests.log 2>&1  || { UNITTESTS_FAILED=true; report_error "$(pwd)/unittests.log"; }
  if ! $UNITTESTS_FAILED ; then
    echo "Done"
  fi
else
  echo "Skipped"
fi


################################################################################

if [ $FAILURES -ne 0 ]; then
  echo ""
  echo ""
  echo ""
  echo "SUMMARY"
  echo "-------"
  echo ""
  echo -n "CPPLINT ------------ "
  check_failure $CPPLINT_FAILED
  echo -n "BUILD -------------- "
  check_failure $BUILD_FAILED
  if running_on_linux; then
    echo -n "UNITTESTS ---------- "
    check_failure $UNITTESTS_FAILED
  fi

  echo ""
  echo ""
  echo "Please check the corresponding logs and fix the error(s)"
fi

exit $FAILURES

