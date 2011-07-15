#!/bin/sh

SUBJECT="CernVM-FS Heartbeat Failure on $HOSTNAME"
RCPT="jblomer@cern.ch"

if [ ! -f /etc/cvmfs/server.conf ]; then
  echo "Failed to include /etc/cvmfs/server.conf" | mail -s "$SUBJECT" $RCPT
  exit 1
fi

. /etc/cvmfs/server.conf
[ -f /etc/cvmfs/server.local ] && . /etc/cvmfs/server.local

for reqvar in SHADOW_DIR LOG_FILE
do
  eval value=\$$reqvar
  if [ -z "$value" ]; then
    echo "Set a value for $reqvar in /etc/cvmfs/server.local" | mail -s "$SUBJECT" $RCPT
    exit 1
  fi
done


NLINES=`cat $LOG_FILE | wc -l`
touch $SHADOW_DIR/.cvmfscatalog > /dev/null 2>&1
sleep 1
NLINES_CHECK=`cat $LOG_FILE | wc -l`
if [ ! $NLINES -lt $NLINES_CHECK ]; then
  LOCKDOWN=`cat /sys/fs/redirfs/filters/cvmfsflt/lockdown` 
  ERROR_MSG="WARNING: cvmfsd service seems to have stopped on $HOSTNAME ($SHADOW_DIR) (Lockdown: $LOCKDOWN)"
  echo "$ERROR_MSG" | mail -s "$SUBJECT" $RCPT
fi

exit 0
