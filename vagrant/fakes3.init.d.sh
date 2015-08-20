#! /bin/sh
# /etc/init.d/fakes3

### BEGIN INIT INFO
# Provides:          fakes3
# Required-Start:    $local_fs
# Required-Stop:
# Should-Start:      fakes3 --port 4567 --root /tmp/fakes3
# Should-Stop:       fakes3 --port 4567 --root /tmp/fakes3
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: A lightweight server clone of Amazon S3
# Description:       fakes3 simulates most of the commands supported by S3 with minimal dependencies
### END INIT INFO

set -e

DAEMON=/usr/bin/fakes3
OPTIONS="--port 13337 --root /tmp/fakes3/docroot"
NAME=fakes3
PIDFILE=/var/run/$NAME.pid
LOGFILE=/var/log/$NAME.log

test -x $DAEMON || exit 0

. /lib/lsb/init-functions

case "$1" in
  start)
    SUCCESS_MSG="Starting FakeS3"
    [ ! -f $PIDFILE ] || { log_failure_msg $SUCCESS_MSG; exit 1; }
    nohup $DAEMON $OPTIONS > $LOGFILE 2>&1 &
    echo $! > $PIDFILE
    log_success_msg $SUCCESS_MSG
    ;;
  stop)
    SUCCESS_MSG="Stopping FakeS3"
    [ -f $PIDFILE ] || { log_failure_msg $SUCCESS_MSG; exit 1; }
    kill -2 $(cat $PIDFILE)
    rm -f $PIDFILE
    log_success_msg $SUCCESS_MSG
    ;;
  status)
    if [ -f $PIDFILE ]; then
      echo "FakeS3 running ($(cat $PIDFILE))"
      exit 0
    else
      echo "FakeS3 stopped"
      exit 1
    fi
    ;;
  *)
    echo "Usage: /etc/init.d/$NAME {start|stop}" >&2
    exit 1
    ;;
esac

exit 0
