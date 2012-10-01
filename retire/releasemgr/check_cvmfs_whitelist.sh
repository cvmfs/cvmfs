#!/bin/sh
# Whitelist check for Nagios

STATE_OK=0
STATE_WARNING=1
STATE_CRITICAL=2
STATE_UNKNOWN=3
STATE_DEPENDENT=4

usage() {
  echo "check_cvmfs_whitelist <URL> <Warning period in days>"
  echo "Example: ./check_cvmfs_whitelist.sh http://cernvm-webfs.cern.ch/opt/alice/.cvmfswhitelist?v=2 4"  
}


URL=$1
WARNING_PERIOD_DAYS=$2

if [ -z $URL ]; then
  usage
  exit $STATE_UNKNOWN
fi

expires=`/usr/bin/curl $URL 2>/dev/null | /usr/bin/head -2 | /usr/bin/tail -1 | /usr/bin/tail -c 15`
if [ $? -ne 0 ]; then
  echo "CRITICAL: Failed to download whitelist from $URL"
  exit $STATE_CRITICAL
fi

year=`echo $expires | /usr/bin/head -c4`
month=`echo $expires | /usr/bin/head -c6 | /usr/bin/tail -c2`
day=`echo $expires | /usr/bin/head -c8 | /usr/bin/tail -c2`
expires_fmt="${year}-${month}-${day}"
expires_num=`/bin/date -u -d $expires_fmt +%s`
if [ $? -ne 0 ]; then
  echo "CRITICAL: Failed to parse whitelist from $URL"
  exit $STATE_CRITICAL
fi

now=`/bin/date -u +%s`
if [ $? -ne 0 ]; then
  echo "CRITICAL: Failed to get current date"
  exit $STATE_CRITICAL
fi

if [ $now -gt $expires_num ]; then
  echo "CRITICAL: Whitelist at $URL has expired"
  exit $STATE_CRITICAL
fi

warning=$[$expires_num-$WARNING_PERIOD_DAYS*24*3600]
if [ $now -gt $warning ]; then
  echo "WARNING: Whitelist at $URL expires in less than $WARNING_PERIOD_DAYS days"
  exit $STATE_WARNING
fi

valid_time=$[($expires_num-$now)/(3600*24)]
echo "OK: Whitelist is valid for another $valid_time days"
exit $STATE_OK

