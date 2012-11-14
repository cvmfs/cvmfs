#!/bin/sh

usage() {
  echo "$0 <logfile> [<test list>]"
}

logfile=$1
if [ -z $logfile ]; then
  usage
  exit 1
fi
logfile=$(pwd)/$(basename $logfile)

shift
testsuite=$@
if [ -z "$testsuite" ]; then
  testsuite=$(find src -mindepth 1 -maxdepth 1 -type d | sort)
fi

echo "Start test suite for cvmfs $(cvmfs2 --version)" > $logfile
date >> $logfile

. ./test_functions
num_failures=0
for t in $testsuite
do
  cvmfs_clean || exit 2
  rm -rf scratch && mkdir scratch || exit 3
  . $t/main || exit 4
  echo "-- Testing $t" >> $logfile
  echo -n "Testing ${cvmfs_test_name}... "
  sh -c ". ./test_functions && . $t/main && cd scratch && cvmfs_run_test $logfile && exit $?"
  RETVAL=$?
  if [ $RETVAL -eq 0 ]; then
    echo "OK"
  else
    echo "Failed!"
    echo "Test failed with RETVAL $RETVAL" >> $logfile
    num_failures=$(($num_failures+1))
  fi
done

date >> $logfile
echo "Finished test suite" >> $logfile

if [ $num_failures -ne 0 ]; then
  echo "$num_failures tests failed!"
fi

exit $num_failures

