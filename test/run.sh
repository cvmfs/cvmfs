#!/bin/sh

usage() {
  echo "$0 <logfile> [<test list> | -x <exclusion list>]"
}


# set up a log file
logfile=$1
if [ -z $logfile ]; then
  usage
  exit 1
fi
if ! echo "$logfile" | grep -q ^/; then
  logfile=$(pwd)/$(basename $logfile)
fi
echo "Start test suite for cvmfs $(cvmfs2 --version)" > $logfile
date >> $logfile

# configure the test set to run
shift
exclusions=""
if [ x$1 = "x-x" ]; then
  shift
  exclusions=$@
else
  testsuite=$@
fi
exclusions="$exclusions $CVMFS_TEST_EXCLUDE"
if [ -z "$testsuite" ]; then
  testsuite=$(find src -mindepth 1 -maxdepth 1 -type d | sort)
fi

TEST_ROOT=$(readlink -f $(dirname $0))
export TEST_ROOT

num_tests=0
num_skipped=0
num_failures=0
num_warnings=0

# test failure handling
# this functions should EXCLUSIVELY be called in the run loop (global state)
report_failure() {
  local message=$1
  local workdir=$2
  echo $message
  if [ x$workdir != x ]; then
    sudo cp $CVMFS_TEST_SYSLOG_TARGET $workdir
  fi
  num_failures=$(($num_failures+1))
}
report_warning() {
  local message=$1
  echo $message
  num_warnings=$(($num_warnings+1))
}
report_skipped() {
  local message=$1
  echo $message
  num_skipped=$(($num_skipped+1))
}

# makes sure the test environment for the test case is sane
setup_environment() {
  local autofs_demand=$1
  local workdir=$2

  # make sure the environment is clean
  if ! cvmfs_clean; then
    echo "failed to clean environment"
    return 101
  fi

  # create a workspace for the test case
  rm -rf "$workdir"
  if ! mkdir -p "$workdir"; then
    echo "failed to create test working directory"
    return 102
  fi

  # configure autofs to the test's needs
  if $autofs_demand; then
    if ! autofs_switch on; then
      echo "failed to switch on autofs"
      return 103
    fi
  else
    if ! autofs_switch off; then
      echo "failed to switch off autofs"
      return 104
    fi
  fi

  # reset the test warning flags
  reset_test_warning_flags

  return 0
}

# source common functions used in the test cases
. ./test_functions

# run the tests
for t in $testsuite
do
  # count the tests
  num_tests=$(($num_tests+1))

  # add some whitespace between tests in the log
  i=0
  while [ $i -lt 10 ]; do
    echo "" >> $logfile
    i=$(($i+1))
  done

  # source the test code
  workdir="${CVMFS_TEST_SCRATCH}/workdir/$t"
  cvmfs_test_autofs_on_startup=true # might be overwritten by some tests
  if ! . $t/main; then
    report_failure "failed to source $t/main" >> $logfile
    continue
  fi

  # write some status info to the screen
  echo "-- Testing ${cvmfs_test_name}" >> $logfile
  echo -n "Testing ${cvmfs_test_name}... "

  # check if test should be skipped
  if contains "$exclusions" $t; then
    report_skipped "test case was marked to be skipped" >> $logfile
    echo "Skipped"
    continue
  fi

  # configure the environment for the test
  if ! setup_environment $cvmfs_test_autofs_on_startup $workdir >> $logfile 2>&1; then
    report_failure "failed to setup environment" >> $logfile
    echo "Failed! (setup)"
    continue
  fi

  # run the test
  sh -c ". ./test_functions                     && \
         . $t/main                              && \
         cd $workdir                            && \
         cvmfs_run_test $logfile                && \
         retval=\$?                             && \
         kill_all_perl_services                 && \
         retval=\$(mangle_test_retval \$retval) && \
         exit \$retval"
  RETVAL=$?

  # check the final test result
  case $RETVAL in
    0)
      rm -rf "$workdir"
      echo "OK"
      ;;
    $CVMFS_MEMORY_WARNING)
      rm -rf "$workdir"
      report_warning "Memory limit exceeded!" >> $logfile
      echo "Memory Warning!"
      ;;
    $CVMFS_TIME_WARNING)
      rm -rf "$workdir"
      report_warning "Time limit exceeded!" >> $logfile
      echo "Time Warning!"
      ;;
    *)
      report_failure "Testcase failed with RETVAL $RETVAL" $workdir >> $logfile
      echo "Failed!"
      ;;
  esac
done

# print final status information
date >> $logfile
echo "Finished test suite" >> $logfile

echo ""
echo "Tests:    $num_tests"
echo "Skipped:  $num_skipped"
echo "Warnings: $num_warnings"
echo "Failures: $num_failures"

exit $num_failures
