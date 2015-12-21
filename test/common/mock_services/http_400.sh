#!/bin/sh

PORT=$1

tmpfile=$(mktemp /tmp/cvmfs-test.XXXXX)
rm -f $tmpfile
mkfifo $tmpfile
trap "rm -f $tmpfile" HUP INT QUIT TERM EXIT
while true; do
  cat $tmpfile | nc -l localhost $PORT | (
    while read; do
      line=$(echo $REPLY | sed 's/[^A-Za-z0-9/*:._ -]//g')
      [ "x$line" = "x" ] && break;
    done
    echo -en "HTTP/1.1 400 Bad Request\r\n\r\n" > $tmpfile
  )
done;
rm -f $tmpfile
