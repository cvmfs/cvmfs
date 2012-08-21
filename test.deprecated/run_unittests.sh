#!/bin/sh

usage() {
  echo "$0 <logfile> [<test list>]"
}

logfile=$1
if [ -z $logfile ]; then
  usage
  exit 1
fi

if [ -x $logfile ]; then
  echo "Won't overwrite executable!"
  usage
  exit 1
fi

shift
testsuite=$@
if [ -z "$testsuite" ]; then
  testsuite=`find unittests -maxdepth 1 -type f | sort`
fi

echo "Start test suite" > $logfile
date >> $logfile

FAILURES=0
for t in $testsuite
do
  echo "-- Testing $t" >> $logfile
  g++ -I ../cvmfs/src -o executable $t || exit 2
  echo -n "Testing ${t}... "
  ./executable >> $logfile
  
  RETVAL=$?
  if [ $RETVAL -eq 0 ]; then
    echo "OK"
  else
    echo "Failed!"
    echo "Test failed with RETVAL $RETVAL" >> $logfile
    FAILURES=$[$FAILURES+1]
  fi
done

date >> $logfile
echo "Finished test suite" >> $logfile

if [ $FAILURES -ne 0 ]; then
  echo "$FAILURES tests failed!"
fi

rm -f executable

exit $RETVAL
