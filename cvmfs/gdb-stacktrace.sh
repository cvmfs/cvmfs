#!/bin/sh

# This script is almost identical to /usr/bin/gstack.
# It is used by monitor.cc on Linux and MacOS X.

# Note: this script was taken from the ROOT svn repository
#       and slightly adapted to print a stacktrace to syslog instead
#       of a temporary file.

tempname=`basename $0 .sh`

if test $# -lt 2; then
   echo "Usage: ${tempname} <executable> <process-id>" 1>&2
   exit 1
fi

if [ `uname -s` = "Darwin" ]; then

   if test ! -x $1; then
      echo "${tempname}: process $1 not found." 1>&2
      exit 1
   fi

   TMPFILE=`mktemp -q /tmp/${tempname}.XXXXXX`
   if test $? -ne 0; then
      echo "${tempname}: can't create temp file, exiting..." 1>&2
      exit 1
   fi

   backtrace="thread apply all bt"
   echo $backtrace > $TMPFILE

   GDB=${GDB:-gdb}

   # Run GDB, strip out unwanted noise.
   $GDB -q -batch -n -x $TMPFILE $1 $2 2>&1  < /dev/null |
   /usr/bin/sed -n \
    -e 's/^(gdb) //' \
    -e '/^#/p' \
    -e 's/\(^Thread.*\)/@\1/p' | tr "@" "\n" > /dev/stdout

   rm -f $TMPFILE

else

   if test ! -r /proc/$2; then
      echo "${tempname}: process $2 not found." 1>&2
      exit 1
   fi

   # GDB doesn't allow "thread apply all bt" when the process isn't
   # threaded; need to peek at the process to determine if that or the
   # simpler "bt" should be used.
   # The leading spaces are needed for have_eval_command's replacement.

   backtrace="bt"
   if test -d /proc/$2/task ; then
      # Newer kernel; has a task/ directory.
      if test `ls /proc/$2/task | wc -l` -gt 1 2>/dev/null ; then
         backtrace="thread apply all bt"
      fi
   elif test -f /proc/$2/maps ; then
      # Older kernel; go by it loading libpthread.
      if grep -e libpthread /proc/$2/maps > /dev/null 2>&1 ; then
         backtrace="thread apply all bt"
      fi
   fi

   GDB=${GDB:-gdb}

   # Run GDB, strip out unwanted noise.
   have_eval_command=`gdb --help 2>&1 |grep eval-command`
   if ! test "x$have_eval_command" = "x"; then
      $GDB --batch --eval-command="$backtrace" /proc/$2/exe $2 2>&1 < /dev/null |
      /bin/sed -n \
         -e 's/^(gdb) //' \
         -e '/^#/p' \
         -e '/^   /p' \
         -e 's/\(^Thread.*\)/@\1/p' | tr '@' '\n' > /dev/stdout
   else
      $GDB -q -n /proc/$2/exe $2 <<EOF 2>&1 |
   $backtrace
EOF
      /bin/sed -n \
         -e 's/^(gdb) //' \
         -e '/^#/p' \
         -e '/^   /p' \
         -e 's/\(^Thread.*\)/@\1/p' | tr '@' '\n' > /dev/stdout
   fi
fi