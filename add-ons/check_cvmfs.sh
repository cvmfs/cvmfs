#!/bin/bash
# CernVM-FS check for Nagios
# Last modified: 27.07.2011
# Bugs and comments to Jakob Blomer (jblomer@cern.ch)

STATUS_OK=0          
STATUS_WARNING=1     # CernVM-FS resource consumption high or 
                    # previous error STATUS detected
STATUS_CRITICAL=2    # CernVM-FS not working
STATUS_UNKNOWN=3     # Internal or usage error

BRIEF_INFO="OK"
RETURN_STATUS=$STATUS_OK


usage() {
   echo "Usage: $0 <repository name> [expected cvmfs version]"
}

help() {
   echo "CernVM-FS check for Nagios"
   usage
}

# Make a version in the format RELEASE.MAJOR.MINOR.PATCH
sanitize_version() {
   local version; version=$1
   
   ndots=`echo $version | tr -Cd . | wc -c`
   while [ $ndots -lt 3 ]; do
      version="${version}.0"
      ndots=`echo $version | tr -Cd . | wc -c`
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



# Option handling
while getopts "h" opt; do
  case $opt in
    h)
      shift 1
      help
      exit $STATUS_OK
    ;;
    *)
      echo "SERVICE STATUS: Invalid option: $1"
      exit $STATUS_UNKNOWN 
      ;;
  esac
done

REPOSITORY=$1
VERSION_EXPECTED=$2

if [ -z "$REPOSITORY" ]; then
   usage
   exit $STATUS_UNKNOWN    
fi

[ -f /etc/cvmfs/default.conf ] && . /etc/cvmfs/default.conf
[ -f /etc/cvmfs/default.local ] && . /etc/cvmfs/default.local
if [ "x$CVMFS_CACHE_BASE" == "x" ]; then
   echo "SERVICE STATUS: CVMFS_CACHE_BASE parameter missing"
   exit $STATUS_UNKNOWN
fi


# Gather information and check for basic availability
CVMFS_INFO=`cvmfs_config stat $REPOSITORY`
RETVAL=$?
if [ $RETVAL -ne 0 ]; then
   BRIEF_INFO="Failed ($RETVAL)"
   [ $RETVAL -lt 32 ] && BRIEF_INFO="repository $REPOSITORY not configured ($RETVAL)"
   
   echo "SERVICE STATUS: $BRIEF_INFO"
   exit $STATUS_CRITICAL
fi
CVMFS_INFO=`echo "$CVMFS_INFO" | tail -n 1`


# Check for CernVM-FS version
VERSION_LOADED=`echo $CVMFS_INFO | cut -d" " -f1`
VERSION_LOADED=`echo $CVMFS_INFO | cut -d" " -f1`
VERSION_INSTALLED=`cvmfs2 --version 2>&1 | cut -d" " -f3`
VERSION_INSTALLED=`sanitize_version $VERSION_INSTALLED`
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
NIOERR=`echo $CVMFS_INFO | cut -d" " -f12`
if [ $NIOERR -gt 0 ]; then
   append_info "$NIOERR I/O errors detected"
   RETURN_STATUS=$STATUS_WARNING
fi

# Check for number of open file descriptors
NFDUSE=`echo $CVMFS_INFO | cut -d" " -f10`
NFDMAX=`echo $CVMFS_INFO | cut -d" " -f11`
FDRATIO=$[$NFDUSE*100/$NFDMAX]
if [ $FDRATIO -gt 80 ]; then
   append_info "low on open file descriptors (${FDRATIO}%)"
   RETURN_STATUS=$STATUS_WARNING
fi

# Check for memory footprint
MEM=`echo $CVMFS_INFO | cut -d" " -f4`
MEM=$[$MEM/1024]
if [ $MEM -gt 50 ]; then
   append_info "high memory consumption (${MEM}m)"
   RETURN_STATUS=$STATUS_WARNING
fi

# Check for number of loaded catalogs
NCATALOGS=`echo $CVMFS_INFO | cut -d" " -f7`
if [ $NCATALOGS -gt 750 ]; then
   append_info "high no. loaded catalogs (${NCATALOGS})"
   RETURN_STATUS=$STATUS_WARNING
fi

# Check for free space on cache partition
DF_CACHE=`df -P "$CVMFS_CACHE_BASE"`
if [ $? -ne 0 ]; then
   append_info "failed to run $CVMFS_CACHE_BASE"
   RETURN_STATUS=$STATUS_CRITICAL
else
   FILL_RATIO=`echo "$DF_CACHE" | tail -n 1 | awk '{print $5}' | tr -Cd [:digit:]`
   if [ $FILL_RATIO -gt 95 ]; then
      append_info "space on cache partition low"
      RETURN_STATUS=$STATUS_WARNING
   fi
fi

# Check for network access
CONNECTIVITY=`echo $CVMFS_INFO | cut -d" " -f19`
if [ $CONNECTIVITY -eq 0 ]; then
   HOST=`echo $CVMFS_INFO | cut -d" " -f17`
   PROXY=`echo $CVMFS_INFO | cut -d" " -f18`
   append_info "offline ($HOST via $PROXY)"
   RETURN_STATUS=$STATUS_CRITICAL
fi

# Check for reading a file
if [ -f "/cvmfs/${REPOSITORY}/.cvmfsdirtab" ]; then
   cat "/cvmfs/${REPOSITORY}/.cvmfsdirtab" > /dev/null 2>&1
   if [ $? -ne 0 ]; then
      append_info "failed to read .cvmfsdirtab from repository"
      RETURN_STATUS=$STATUS_CRITICAL
   fi
fi

REVISION=`echo $CVMFS_INFO | cut -d" " -f5`
echo "SERVICE STATUS: $BRIEF_INFO; repository revision $REVISION"
exit $RETURN_STATUS
