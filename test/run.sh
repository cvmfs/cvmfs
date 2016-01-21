#!/bin/bash

usage() {
  echo "$0 <logfile> [-o xUnit XML output] [-x <exclusion list> --] [test list]"
}

export LC_ALL=C

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

# read command line paramters
shift
test_exclusions=0
xml_output=""
debug=""
while getopts "xo:d" option; do
  case $option in
    x)
      test_exclusions=1
    ;;
    o)
      xml_output="$OPTARG"
    ;;
    d)
      debug="-x"
    ;;
    ?)
      usage
      exit 1
    ;;
  esac
done
shift $(( $OPTIND - 1 ))

# configure the exclusion list of tests
exclusions="$CVMFS_TEST_EXCLUDE"
if [ $test_exclusions -ne 0 ]; then
  while [ $# -ne 0 ] && [ x"$1" != x"--" ]; do
    exclusions="$exclusions $1"
    shift
  done
  shift # get rid of '--'
fi

testsuite="$@"
if [ -z "$testsuite" ]; then
  testsuite=$(find src -mindepth 1 -maxdepth 1 -type d | sort)
fi

# start running the tests
TEST_ROOT=$(cd "$(dirname "$0")"; pwd)
export TEST_ROOT

num_tests=0
num_skipped=0
num_passed=0
num_failures=0
num_warnings=0

# test failure handling
# this functions should EXCLUSIVELY be called in the run loop (global state)
report_failure() {
  local message="$1"
  local workdir=$2
  echo $message
  if [ x$workdir != x ]; then
    if [ x$PARROT_ENABLED = "xTRUE" ]; then
      cp $CVMFS_TEST_SYSLOG_TARGET $workdir
    else
      sudo cp $CVMFS_TEST_SYSLOG_TARGET $workdir
    fi
  fi
  num_failures=$(($num_failures+1))
}
report_warning() {
  local message="$1"
  echo $message
  num_warnings=$(($num_warnings+1))
}
report_passed() {
  local message="$1"
  echo $message
  num_passed=$(($num_passed+1))
}
report_skipped() {
  local message="$1"
  echo $message
  num_skipped=$(($num_skipped+1))
}

clean_workdir() {
  if [ x$PARROT_ENABLED = "xTRUE" ]; then
    rm -rf "$workdir" >> $logfile
  else
    sudo rm -rf "$workdir" >> $logfile
  fi
}

# makes sure the test environment for the test case is sane
setup_environment() {
  local autofs_demand=$1
  local workdir=$2

  # make sure the environment is clean
  local clean_retval=0
  cvmfs_clean || clean_retval=$?
  if [ $clean_retval -ne 0 ]; then
    echo "failed to clean environment (retval: $clean_retval)"
    return 101
  fi

  # create a workspace for the test case
  rm -rf "$workdir" && mkdir -p "$workdir"
  if [ $? -ne 0 ]; then
    echo "failed to create test working directory"
    return 102
  fi

  # if we are not inside a docker
  if [ x"$CVMFS_TEST_DOCKER" = xno ] && ! running_on_osx; then
    # configure autofs to the test's needs
    service_switch autofs restart || true
    local timeout=10 # wait until autofs restarts (possible race >.<)
    while [ $timeout -gt 0 ] && ! autofs_check; do
      timeout=$(( $timeout - 1))
      sleep 1
    done
    if [ $timeout -eq 0 ]; then
      echo "failed to restart autofs"
      return 103
    fi
    if ! $autofs_demand; then
      if ! autofs_switch off; then
        echo "failed to switch off autofs"
        return 104
      fi
    fi
  fi

  # if the test is a benchmark we have to configure the environment
  if [ x"$cvmfs_benchmark" = x"yes" ]; then
    setup_benchmark_environment $workdir
  fi

  # reset the test warning flags
  reset_test_warning_flags

  return 0
}

# source common functions used in the test cases
. ./test_functions

testsuite_start="$(get_millisecond_epoch)"

workdir_basedir="${CVMFS_TEST_SCRATCH}/workdir"
scratch_basedir="${CVMFS_TEST_SCRATCH}/scratch"
[ ! -d $scratch_basedir ] || rm -fR $scratch_basedir
mkdir -p $scratch_basedir

get_iso8601_timestamp > ${scratch_basedir}/starttime

to_syslog "Test Suite started"

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
  workdir="${workdir_basedir}/$(basename $t)"
  scratchdir="${scratch_basedir}/$(basename $t)"

  if ! mkdir -p $scratchdir; then
    report_failure "failed to create $scratchdir" >> $logfile
    continue
  fi

  wc -l < $logfile > ${scratchdir}/log_begin

  cvmfs_test_autofs_on_startup=true # might be overwritten by some tests
  if ! . $t/main; then
    report_failure "failed to source $t/main" >> $logfile
    echo "101" > ${scratchdir}/retval
    continue
  fi

  # write some status info to the screen
  TEST_NR="$(basename $t | head -c3)"
  to_syslog "Test $TEST_NR (${cvmfs_test_name}) started"
  echo "-- Testing ${cvmfs_test_name} ($(date) / test number $TEST_NR)" >> $logfile
  echo -n "Testing ${cvmfs_test_name}... "
  echo "$cvmfs_test_name"          > ${scratchdir}/name
  echo "$(basename $t | head -c3)" > ${scratchdir}/number

  # check if test should be skipped
  if contains "$exclusions" $t; then
    report_skipped "test case was marked to be skipped" >> $logfile
    echo "Skipped"
    touch ${scratchdir}/skipped
    continue
  fi

  # configure the environment for the test
  setup_retval=0
  setup_environment $cvmfs_test_autofs_on_startup $workdir >> $logfile 2>&1 || setup_retval=$?
  if [ $setup_retval -ne 0 ]; then
    to_syslog "Test $TEST_NR (${cvmfs_test_name}) failed (setup)"
    report_failure "failed to setup environment (retval: $setup_retval)" >> $logfile
    echo "Failed! (setup)"
    echo "0"         > ${scratchdir}/elapsed
    echo "102"       > ${scratchdir}/retval
    wc -l < $logfile > ${scratchdir}/log_end
    touch              ${scratchdir}/failure
    continue
  fi

  # run the test
  test_start=$(get_millisecond_epoch)
  bash $debug -c ". ./test_functions                     && \
                  . $t/main                              && \
                  cd $workdir                            && \
                  cvmfs_run_test $logfile $(pwd)/${t}    && \
                  retval=\$?                             && \
                  retval=\$(mangle_test_retval \$retval) && \
                  exit \$retval" >> $logfile 2>&1
  RETVAL=$?
  test_end=$(get_millisecond_epoch)
  test_time_elapsed=$(( ( $test_end - $test_start ) ))
  echo "execution took $(milliseconds_to_seconds $test_time_elapsed) seconds" >> $logfile
  echo "$test_time_elapsed" > ${scratchdir}/elapsed
  echo "$RETVAL"            > ${scratchdir}/retval

  # if the test is a benchmark we have to collect the results before removing the folder
  if [ x"$cvmfs_benchmark" = x"yes" ]; then
    collect_benchmark_results
    cvmfs_umount $FQRN > /dev/null 2>&1
  fi

  # check the final test result
  case $RETVAL in
    0)
      clean_workdir
      to_syslog "Test $TEST_NR (${cvmfs_test_name}) finished successfully"
      report_passed "Test passed" >> $logfile
      touch ${scratchdir}/success
      echo "OK"
      ;;
    $CVMFS_MEMORY_WARNING)
      clean_workdir
      to_syslog "Test $TEST_NR (${cvmfs_test_name}) finished with memory warning"
      report_warning "Memory limit exceeded!" >> $logfile
      touch ${scratchdir}/memorywarning
      echo "Memory Warning!"
      ;;
    $CVMFS_TIME_WARNING)
      clean_workdir
      to_syslog "Test $TEST_NR (${cvmfs_test_name}) finished with time warning"
      report_warning "Time limit exceeded!" >> $logfile
      tail -n 50 /var/log/messages /var/log.syslog >> $logfile 2>/dev/null
      touch ${scratchdir}/timewarning
      echo "Time Warning!"
      ;;
    $CVMFS_GENERAL_WARNING)
      clean_workdir
      to_syslog "Test $TEST_NR (${cvmfs_test_name}) finished with warning"
      report_warning "Test case finished with warnings!" >> $logfile
      touch ${scratchdir}/generalwarning
      echo "Warning!"
      ;;
    *)
      to_syslog "Test $TEST_NR (${cvmfs_test_name}) failed"
      report_failure "Testcase failed with RETVAL $RETVAL" $workdir >> $logfile
      tail -n 50 /var/log/messages /var/log.syslog >> $logfile 2>/dev/null
      touch ${scratchdir}/failure
      echo "Failed!"
      ;;
  esac

  wc -l < $logfile > ${scratchdir}/log_end
done

testsuite_end="$(get_millisecond_epoch)"
testsuite_time_elapsed=$(( $testsuite_end - $testsuite_start ))

echo "$testsuite_time_elapsed" > ${scratch_basedir}/elapsed
echo "$num_tests"              > ${scratch_basedir}/num_tests
echo "$num_skipped"            > ${scratch_basedir}/num_skipped
echo "$num_passed"             > ${scratch_basedir}/num_passed
echo "$num_warnings"           > ${scratch_basedir}/num_warnings
echo "$num_failures"           > ${scratch_basedir}/num_failures

# export xunit XML
if [ ! -z "$xml_output" ]; then
  export_xunit_xml "$xml_output" $scratch_basedir $logfile
fi

# remove runtime information
# rm -rf $scratch_basedir

# print final status information
date >> $logfile
echo "Finished test suite in $(milliseconds_to_human_readable $testsuite_time_elapsed)" >> $logfile

echo ""
echo "Tests:    $num_tests"
echo "Skipped:  $num_skipped"
echo "Passed:   $num_passed"
echo "Warnings: $num_warnings"
echo "Failures: $num_failures"
echo ""
echo "took $(milliseconds_to_human_readable $testsuite_time_elapsed)"

to_syslog "Test Suite finished"

exit $num_failures

