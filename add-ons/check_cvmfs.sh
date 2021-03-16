#!/bin/bash
# CernVM-FS check for Nagios
# Version 1.10, last modified: 19.06.2017
# Bugs and comments to Jakob Blomer (jblomer@cern.ch)
#
# ChangeLog
# 1.11 - 16.03.2021
#    - Customize max fill ration (contributed by NIKHEF)
# 1.10 - 19.06.2017
#    - Check for cleanup rate within the last 24 hours
# 1.9 - 13.02.2015:
#    - Use --max-time 3*$connect_timeout option in curl requests
#    - Add fallback proxies to network checks
#    - Add -t(imeout) parameter to avoid hanging check.  Defaults to 2 minutes.
#    - Add -i(node check) parameter to check for inodes overflowing 32bit
# 1.8:
#    - resolve auto proxy
# 1.7:
#    - optionally turn on memory verification
# 1.6:
#    - added -H "Pragma:" as curl option (avoid no-cache pragma)
# 1.5:
#    - return STATUS_UNKNOWN if extended attribute cannot be read
#    - return immediately if transport endpoint is not connected
#    - start of ChangeLog

VERSION=1.11

STATUS_OK=0
STATUS_WARNING=1     # Check timed out or CernVM-FS resource consumption high or
                     # previous error status detected
STATUS_CRITICAL=2    # CernVM-FS not working
STATUS_UNKNOWN=3     # Internal or usage error

BRIEF_INFO="OK"
RETURN_STATUS=$STATUS_OK

MAX_FILL_RATIO=95
TIMEOUT_SECONDS=120


usage() {
   /bin/echo "Usage:   $0 [-t <seconds>][-m] [-n] [-f fill_ratio] [-i] <repository name> [expected cvmfs version]"
   /bin/echo "Example: $0 -t 60 -m -n atlas.cern.ch 2.0.4"
   /bin/echo "Options:"
   /bin/echo "  -t  second after which the check times out with a warning (default: ${TIMEOUT_SECONDS})"
   /bin/echo "  -n  run extended network checks"
   /bin/echo "  -m  check memory consumption of the cvmfs2 process"
   /bin/echo "      (less than 50M or 1% of available memory)"
   /bin/echo "  -f  set max fill ratio warning level (default 95)"
   /bin/echo "  -i  check if inodes exceed 32bit which can break 32bit programs"
   /bin/echo "      that use the non-64bit glibc file system interface"
}

version() {
   /bin/echo "CernVM-FS check for Nagios, version: $VERSION"
}

help() {
   version
   usage
}

# Make a version in the format RELEASE.MAJOR.MINOR.PATCH
sanitize_version() {
   local version; version=$1

   ndots=`/bin/echo $version | /usr/bin/tr -Cd . | /usr/bin/wc -c`
   while [ $ndots -lt 3 ]; do
      version="${version}.0"
      ndots=`/bin/echo $version | /usr/bin/tr -Cd . | /usr/bin/wc -c`
   done

   echo $version
}

# Appends information to the brief output string
append_info() {
   local info; info=$1

   if [ "x$BRIEF_INFO" == "xOK" ]; then
      BRIEF_INFO=$info
   else
      BRIEF_INFO="${BRIEF_INFO}; $info"
   fi
}

# Read xattr value from current directory into XATTR_VALUE variable
get_xattr() {
   XATTR_NAME=$1

   XATTR_VALUE=`/usr/bin/attr -q -g $XATTR_NAME .`
   if [ $? -ne 0 ]; then
      /bin/echo "SERVICE STATUS: failed to read $XATTR_NAME attribute"
      exit $STATUS_UNKNOWN
   fi
}

# Try reading the xattr value from current directory, use the provided default
# if the attribute is not available
try_get_xattr() {
   XATTR_NAME=$1
   local default="$2"

   XATTR_VALUE=`/usr/bin/attr -q -g $XATTR_NAME . 2>/dev/null`
   if [ $? -ne 0 ]; then
      XATTR_VALUE="$default"
   fi
}


# Option handling
OPT_NETWORK_CHECK=0
OPT_MEMORY_CHECK=0
OPT_INODE_CHECK=0
while getopts "hVvt:nmf:i" opt; do
  case $opt in
    h)
      help
      exit $STATUS_OK
    ;;
    V)
      version
      exit $STATUS_OK
    ;;
    v)
      /bin/echo "verbose mode"
    ;;
    t)
      TIMEOUT_SECONDS="$(/bin/echo "$OPTARG" | /usr/bin/tr -c -d 0-9)"
      if [ "x${TIMEOUT_SECONDS}" = "x" ]; then
        /bin/echo "SERVICE STATUS: Invalid timeout argument: $OPTARG"
        exit $STATUS_UNKNOWN
      fi
    ;;
    n)
      OPT_NETWORK_CHECK=1
    ;;
    m)
      OPT_MEMORY_CHECK=1
    ;;
    f)
      MAX_FILL_RATIO="$OPTARG"
    ;;
    i)
      OPT_INODE_CHECK=1
    ;;
    *)
      /bin/echo "SERVICE STATUS: Invalid option: $1"
      exit $STATUS_UNKNOWN
      ;;
  esac
done
shift $[$OPTIND-1]

REPOSITORY=$1
VERSION_EXPECTED=$2

if [ -z "$REPOSITORY" ]; then
   usage
   exit $STATUS_UNKNOWN
fi

# The actual probes.  This function can hang if cvmfs hangs and is subject
# to be called with a timeout guard
do_check() {
  # Read repository config
  if [ -f /etc/cvmfs/config.sh ]
  then
    . /etc/cvmfs/config.sh
  else
    /bin/echo "SERVICE STATUS: /etc/cvmfs/config.sh missing"
    exit $STATUS_UNKNOWN
  fi
  cvmfs_readconfig
  if [ $? -ne 0 ]; then
    /bin/echo "SERVICE STATUS: failed to read CernVM-FS configuration"
    exit $STATUS_CRITICAL
  fi
  FQRN=`cvmfs_mkfqrn $REPOSITORY`
  ORG=`cvmfs_getorg $FQRN`
  cvmfs_readconfig $FQRN
  if [ $? -ne 0 ]; then
    /bin/echo "SERVICE STATUS: failed to read $FQRN configuration"
    exit $STATUS_CRITICAL
  fi

  # Grab mountpoint / basic availability
  cd "${CVMFS_MOUNT_DIR}/$FQRN" && ls . > /dev/null
  if [ $? -ne 0 ]; then
    /bin/echo "SERVICE STATUS: failed to access $FQRN"
    exit $STATUS_CRITICAL
  fi

  # Gather usage information from cvmfs
  get_xattr version; VERSION_LOADED=`sanitize_version $XATTR_VALUE`
  VERSION_INSTALLED=`/usr/bin/cvmfs2 --version 2>&1 | /bin/cut -d" " -f3`
  VERSION_INSTALLED=`sanitize_version $VERSION_INSTALLED`
  get_xattr nioerr; NIOERR=$XATTR_VALUE
  get_xattr usedfd; NFDUSE=$XATTR_VALUE
  get_xattr maxfd; NFDMAX=$XATTR_VALUE
  get_xattr nclg; NCATALOGS=$XATTR_VALUE
  get_xattr revision; REVISION=$XATTR_VALUE
  get_xattr pid; PID=$XATTR_VALUE
  MEMKB=0
  if [ $OPT_MEMORY_CHECK -eq 1 ]; then
    MEMKB=`/bin/ps -p $PID -o rss= | /bin/sed 's/ //g'`
    if [ $PIPESTATUS -ne 0 ]; then
      /bin/echo "SERVICE STATUS: failed to read memory consumption"
      exit $STATUS_UNKNOWN
    fi
  fi
  INODE_MAX=0
  if [ $OPT_INODE_CHECK -eq 1 ]; then
    get_xattr inode_max; INODE_MAX=$XATTR_VALUE
  fi
  # The extended attribute was added in cvmfs 2.4.  The -1 value means that
  # querying for the cleanup rate is not supported
  try_get_xattr ncleanup24 -1; NCLEANUP24=$XATTR_VALUE

  # Network settings;  TODO: currently configured values required
  if [ $OPT_NETWORK_CHECK -eq 1 ]; then
    if [ ! -z "$CVMFS_HTTP_PROXY" -a ! -z "$CVMFS_SERVER_URL"  ]; then
      CVMFS_HOSTS=`/bin/echo "$CVMFS_SERVER_URL" | /bin/sed 's/,\|;/ /g' \
         | sed s/@org@/$ORG/g | sed s/@fqrn@/$FQRN/g`
      CVMFS_PROXIES=`/bin/echo "$CVMFS_HTTP_PROXY $CVMFS_FALLBACK_PROXY" | /bin/sed 's/;\||/ /g'`
      CVMFS_REAL_PROXIES=
      for proxy in $CVMFS_PROXIES; do
        if [ "x$proxy" = "xauto" ]; then
          proxy=$(/usr/bin/cvmfs2 __wpad__ auto "$CVMFS_SERVER_URL" 2>/dev/null)
          proxy=$(echo "$proxy" | /bin/sed 's/;/ /g')
        fi
        CVMFS_REAL_PROXIES="$CVMFS_REAL_PROXIES $proxy"
      done
      CVMFS_PROXIES="$CVMFS_REAL_PROXIES"
    else
      /bin/echo "SERVICE STATUS: CernVM-FS configuration error"
      exit $STATUS_UNKNOWN
    fi

    get_xattr timeout; CVMFS_TIMEOUT_PROXY=$XATTR_VALUE
    get_xattr timeout_direct; CVMFS_TIMEOUT_DIRECT=$XATTR_VALUE
  fi

  # Check for CernVM-FS version
  if [ "$VERSION_INSTALLED" != "$VERSION_LOADED" ]; then
    append_info "version mismatch (loaded $VERSION_LOADED, installed $VERSION_INSTALLED)"
    RETURN_STATUS=$STATUS_WARNING
  fi

  if [ "x$VERSION_EXPECTED" != "x" ]; then
    VERSION_EXPECTED=`sanitize_version $VERSION_EXPECTED`
    if [ "$VERSION_EXPECTED" != "$VERSION_INSTALLED" ]; then
      append_info "version mismatch (expected $VERSION_EXPECTED, installed $VERSION_INSTALLED)"
      RETURN_STATUS=$STATUS_WARNING
    fi
  fi

  # Check for previously detected I/O errors
  if [ $NIOERR -gt 0 ]; then
    append_info "$NIOERR I/O errors detected"
    RETURN_STATUS=$STATUS_WARNING
  fi

  # Check for number of open file descriptors
  FDRATIO=$[$NFDUSE*100/$NFDMAX]
  if [ $FDRATIO -gt 80 ]; then
    append_info "low on open file descriptors (${FDRATIO}%)"
    RETURN_STATUS=$STATUS_WARNING
  fi

  # Check for memory footprint (< 50M or < 1% of available memory?)
  if [ $OPT_MEMORY_CHECK -eq 1 ]; then
    MEM=$[$MEMKB/1024]
    if [ $MEM -gt 50 ]; then
      MEMTOTAL=`/bin/grep MemTotal /proc/meminfo | /bin/awk '{print $2}'`
      # More than 1% of total memory?
      if [ $[$MEMKB*100] -gt $MEMTOTAL ]; then
        append_info "high memory consumption (${MEM}m)"
        RETURN_STATUS=$STATUS_WARNING
      fi
    fi
  fi

  # Check for number of loaded catalogs (< 10% of FDMAX?)
  if [ $[$NCATALOGS*10] -gt $NFDMAX ]; then
    append_info "high no. loaded catalogs (${NCATALOGS})"
    RETURN_STATUS=$STATUS_WARNING
  fi

  # Check for free space on cache partition
  DF_CACHE=`/bin/df -P "$CVMFS_CACHE_BASE"`
  if [ $? -ne 0 ]; then
    append_info "failed to run /bin/df -P $CVMFS_CACHE_BASE"
    RETURN_STATUS=$STATUS_CRITICAL
  else
    FILL_RATIO=`/bin/echo "$DF_CACHE" | /usr/bin/tail -n1 | \
                /bin/awk '{print $5}' | /usr/bin/tr -Cd [:digit:]`
    if [ $FILL_RATIO -gt $MAX_FILL_RATIO ]; then
      append_info "space on cache partition low"
      RETURN_STATUS=$STATUS_WARNING
    fi
  fi

  # Network connectivity, all proxy / host combinations
  if [ $OPT_NETWORK_CHECK -eq 1 ]; then
    for HOST in $CVMFS_HOSTS
    do
      for PROXY in $CVMFS_PROXIES
      do
        if [ $PROXY != "DIRECT" ]; then
          PROXY_ENV="env http_proxy=$PROXY"
          TIMEOUT=$CVMFS_TIMEOUT
        else
          PROXY_ENV=
          TIMEOUT=$CVMFS_TIMEOUT_DIRECT
        fi
        URL="${HOST}/.cvmfspublished"
        $PROXY_ENV /usr/bin/curl -H "Pragma:" -f \
          --max-time $((3*${TIMEOUT})) \
          --connect-timeout $TIMEOUT $URL > \
          /dev/null 2>&1
        if [ $? -ne 0 ]; then
          append_info "offline ($HOST via $PROXY)"
          RETURN_STATUS=$STATUS_WARNING
        fi
      done
    done
  fi

  # Check for inode numbers >32bit
  if [ $OPT_INODE_CHECK -eq 1 ]; then
    OVERFLOW=$(/usr/bin/perl -e "$INODE_MAX > 2147483647 ? print 1 : print 0" 2>/dev/null)
    if [ $OVERFLOW -eq 1 ]; then
      append_info "inodes exceed 32bit, some 32bit applications might break"
      RETURN_STATUS=$STATUS_WARNING
    fi
  fi

  # Check for number of cache cleanups within the last 24 hours
  if [ $NCLEANUP24 -gt 24 ]; then
    append_info "frequent cache cleanups, cache might be undersized"
    RETURN_STATUS=$STATUS_WARNING
  fi

  if [ -f "/cvmfs/${REPOSITORY}/.cvmfsdirtab" ]; then
    cat "/cvmfs/${REPOSITORY}/.cvmfsdirtab" > /dev/null 2>&1
    if [ $? -ne 0 ]; then
      append_info "failed to read .cvmfsdirtab from repository"
      RETURN_STATUS=$STATUS_CRITICAL
    fi
  fi

  /bin/echo "SERVICE STATUS: $BRIEF_INFO; repository revision $REVISION"
  exit $RETURN_STATUS
}


# Guard the check by a timeout
do_check &
PID=$!
START_TIME=$SECONDS
while [ $((${SECONDS}-${START_TIME})) -lt ${TIMEOUT_SECONDS} ]; do
  /bin/sleep 1
  kill -s 0 $PID >/dev/null 2>&1
  if [ $? -ne 0 ]; then
    wait $PID
    exit $?
  fi
done

kill -s 9 $PID >/dev/null
/bin/echo "SERVICE STATUS: timeout after ${TIMEOUT_SECONDS}s"
exit $STATUS_WARNING
