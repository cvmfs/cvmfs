#!/bin/sh

SUBJECT="CernVM-FS Status Report on $HOSTNAME"
RCPT=jblomer@cern.ch

if [ ! -f /etc/cvmfs/server.conf ]; then
  echo "Failed to include /etc/cvmfs/server.conf" | mail -s "$SUBJECT (Failure)" $RCPT
  exit 1
fi
. /etc/cvmfs/server.conf
[ -f /etc/cvmfs/server.local ] && . /etc/cvmfs/server.local

for reqvar in SHADOW_DIR PUB_DIR
do
   eval value=\$$reqvar
   if [ -z "$value" ]; then
      echo "Set a value for $reqvar in /etc/cvmfs/server.local" | mail -s "$SUBJECT (Failure)" $RCPT
      exit 1
   fi
done


cd "$PUB_DIR/catalogs" 
if [ $? -ne 0 ]; then
  echo "Failed to change directory to $PUB_DIR/catalogs" | mail -s "$SUBJECT (Failure)" $RCPT
  exit 1
fi
TIMESTAMP=`date`
REPOSITORY=`basename "$SHADOW_DIR"`

OUTPUT=`mktemp /tmp/cvmfs.XXXXXX`
if [ $? -ne 0 ]; then
  echo "Failed to create output temp file" | mail -s "$SUBJECT (Failure)" $RCPT
  exit 1
fi

echo "Start integrity check for $REPOSITORY on $HOSTNAME at $TIMESTAMP" > $OUTPUT
for r in `find . -name .cvmfscatalog.working`
do
  EXE=/usr/bin/cvmfs_clgcmp
  if [ -x /usr/local/bin/cvmfs_clgcmp ]; then
    EXE=/usr/local/bin/cvmfs_clgcmp
  fi
  CMD="$EXE $r ${SHADOW_DIR}/`dirname $r`"
  echo $CMD >> $OUTPUT
  $CMD >> $OUTPUT 2>&1
done
TIMESTAMP=`date`
echo "Finished integrity check on $TIMESTAMP" >> $OUTPUT

echo >> $OUTPUT
echo "ls -lah:" >> $OUTPUT
find . -name .cvmfscatalog | xargs ls -lahH >> $OUTPUT  

mail -s "$SUBJECT" $RCPT < $OUTPUT
rm -f $OUTPUT

