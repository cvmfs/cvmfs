#!/bin/bash
# CernVM-FS check for Nagios
# Version 1.4, last modified: 01.11.2011
# Bugs and comments to Jakob Blomer (jblomer@cern.ch)

VERSION=1.4

STATUS_OK=0          
STATUS_WARNING=1     # CernVM-FS resource consumption high or 
                     # previous error STATUS detected
STATUS_CRITICAL=2    # CernVM-FS not working
STATUS_UNKNOWN=3     # Internal or usage error

BRIEF_INFO="OK"
RETURN_STATUS=$STATUS_OK


usage() {
   /bin/echo "Usage:   $0 [-n] <repository name> [expected cvmfs version]"
   /bin/echo "Example: $0 -n atlas.cern.ch 2.0.4"
   /bin/echo "Options:"
   /bin/echo "  -n  run extended network checks"
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
      exit $SERVICE_UNKNOWN
   fi
}




# Option handling
OPT_NETWORK_CHECK=0
while getopts "hVvn" opt; do
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
    n)
      OPT_NETWORK_CHECK=1
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
cd "${CVMFS_MOUNT_DIR}/$FQRN"
if [ $? -ne 0 ]; then
  /bin/echo "SERVICE STATUS: failed to access $FQRN"
  exit $STATUS_CRITICAL
fi


# Gather information
get_xattr version; VERSION_LOADED=`sanitize_version $XATTR_VALUE`
VERSION_INSTALLED=`/usr/bin/cvmfs2 --version 2>&1 | /bin/cut -d" " -f3`
VERSION_INSTALLED=`sanitize_version $VERSION_INSTALLED`
get_xattr nioerr; NIOERR=$XATTR_VALUE
get_xattr usedfd; NFDUSE=$XATTR_VALUE
get_xattr maxfd; NFDMAX=$XATTR_VALUE
get_xattr nclg; NCATALOGS=$XATTR_VALUE
get_xattr revision; REVISION=$XATTR_VALUE
get_xattr pid; PID=$XATTR_VALUE
MEMKB=`/bin/ps -p $PID -o rss= | /bin/sed 's/ //g'`
if [ $PIPESTATUS -ne 0 ]; then
   /bin/echo "SERVICE STATUS: failed to read memory consumption"
   exit $STATUS_UNKNOWN
fi


# Network settings;  TODO: currently configured values required
if [ $OPT_NETWORK_CHECK -eq 1 ]; then
   if [ ! -z "$CVMFS_HTTP_PROXY" -a ! -z "$CVMFS_SERVER_URL"  ]; then
      CVMFS_HOSTS=`/bin/echo "$CVMFS_SERVER_URL" | /bin/sed 's/,\|;/ /g' \
         | sed s/@org@/$ORG/g | sed s/@fqrn@/$FQRN/g`
      CVMFS_PROXIES=`/bin/echo "$CVMFS_HTTP_PROXY" | /bin/sed 's/;\||/ /g'`
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
MEM=$[$MEMKB/1024]
if [ $MEM -gt 50 ]; then
   MEMTOTAL=`/bin/grep MemTotal /proc/meminfo | /bin/awk '{print $2}'`
   # More than 1% of total memory?
   if [ $[$MEMKB*100] -gt $MEMTOTAL ]; then
      append_info "high memory consumption (${MEM}m)"
      RETURN_STATUS=$STATUS_WARNING
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
   if [ $FILL_RATIO -gt 95 ]; then
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
         $PROXY_ENV /usr/bin/curl -f --connect-timeout $TIMEOUT $URL > \
           /dev/null 2>&1
         if [ $? -ne 0 ]; then
            append_info "offline ($HOST via $PROXY)"
            RETURN_STATUS=$STATUS_WARNING
         fi
      done
   done
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
