#!/bin/sh
# Nagios check, first check HTTP availablilty then check whitelist expiry

STATE_OK=0
STATE_WARNING=1
STATE_CRITICAL=2
STATE_UNKNOWN=3
STATE_DEPENDENT=4

while getopts "H:A:u:e:w:c:t:E:l:" opt; do
  case $opt in
    H)
      HOST_ADDR=$OPTARG
      ;;
    A)
      USER_AGENT=$OPTARG
      ;;
    u)
      BASE_URL=$OPTARG
      ;;
    e)
      EXPECTED=$OPTARG
      ;;
    w)
      WARNING_TIME=$OPTARG
      ;;
    c)
      CRITICAL_TIME=$OPTARG
      ;;
    t)
      TIMEOUT=$OPTARG
      ;;
    E)
      EXPIRY_WARNING=$OPTARG
      ;;
    l)
      WHITELIST_URL=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit STATE_UNKNOWN 
      ;;
  esac
done

CHECK_HTTP=`dirname $0`/check_http
HTTP_OUTPUT=`$CHECK_HTTP -H $HOST_ADDR -A "$USER_AGENT" -u $BASE_URL -e $EXPECTED -w $WARNING_TIME -c $CRITICAL_TIME -t $TIMEOUT`
RESULT=$?

if [ $RESULT -eq 0 ]; then
  CHECK_WHITELIST=`dirname $0`/check_cvmfs_whitelist.sh
  WHITELIST_OUTPUT=`$CHECK_WHITELIST $WHITELIST_URL $EXPIRY_WARNING`
  RESULT=$?
fi

echo "`echo $HTTP_OUTPUT | /bin/sed 's/\([^|]*\).*/\1/'` $WHITELIST_OUTPUT"

exit $RESULT


