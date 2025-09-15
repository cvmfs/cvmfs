################################################################################
#                                                                              #
#                              Environment Setup                               #
#                                                                              #
################################################################################

# Configuration variables for update-geodb -l.  May be overridden in
#   /etc/cvmfs/cvmfs_server_hooks.sh, /etc/cvmfs/server.local, or
#   per-repo in replica.conf.
# Default settings will attempt to update from cvmfs_server snapshot
#   in the 10 o'clock hour of Tuesday, every 2 weeks for openhtc or
#   every 4 weeks for maxmind.
CVMFS_GEO_AUTO_UPDATE=true # Automatically update from cvmfs_server snapshot
CVMFS_UPDATEGEO_SOURCE=openhtc # Database source: openhtc, maxmind, or none
CVMFS_UPDATEGEO_DAY=2   # Weekday of update, 0-6 where 0 is Sunday, default Tuesday
CVMFS_UPDATEGEO_HOUR=10 # First hour of day for update, 0-23, default 10am
CVMFS_UPDATEGEO_MINDAYS= # Minimum days between update attempts (set below)
CVMFS_UPDATEGEO_MAXDAYS= # Maximum days before considering it urgent (set below)

CVMFS_UPDATEGEO_DIR="/var/lib/cvmfs-server/geo" # Directory to download into
CVMFS_UPDATEGEO_DB=      # DB name (set below)
CVMFS_UPDATEGEO_OLDDB=   # DB name to remove if present (set below)
CVMFS_UPDATEGEO_URLBASE= # Base of URL for download (set below)
CVMFS_UPDATEGEO_URLSUFFIX= # Suffix to add to URLBASE for download

DEFAULT_LOCAL_STORAGE="/srv/cvmfs"

LATEST_JSON_INFO_SCHEMA=1

# Should we publish CVMFS and OS versions in meta.json?
# Set to any non-true string to turn off this feature.
CVMFS_PUBLISH_VERSIONS_IN_META_FILE=true

if [ -f /etc/cvmfs/server.local ]; then
  if [ -r /etc/cvmfs/server.local ]; then
    . /etc/cvmfs/server.local
  else
    echo "WARNING: cannot read /etc/cvmfs/server.local" >&2
  fi
fi

# setup server hooks: no-ops (overridable by /etc/cvmfs/cvmfs_server_hooks.sh)
transaction_before_hook() { :; }
transaction_after_hook() { :; }
abort_before_hook() { :; }
abort_after_hook() { :; }
publish_before_hook() { :; }
publish_after_hook() { :; }

cvmfs_sys_file_is_regular /etc/cvmfs/cvmfs_server_hooks.sh && . /etc/cvmfs/cvmfs_server_hooks.sh

if [ "$CVMFS_UPDATEGEO_SOURCE" = "openhtc" ]; then
  CVMFS_UPDATEGEO_MINDAYS=${CVMFS_UPDATEGEO_MINDAYS:-7}
  CVMFS_UPDATEGEO_MAXDAYS=${CVMFS_UPDATEGEO_MAXDAYS:-14}
  CVMFS_UPDATEGEO_OLDDB=GeoLite2-City.mmdb
  CVMFS_UPDATEGEO_DB="${CVMFS_UPDATEGEO_DB:-iplocation.mmdb}"
  CVMFS_UPDATEGEO_URLBASE="${CVMFS_UPDATEGEO_URLBASE:-https://geoipdb.openhtc.io}"
  CVMFS_UPDATEGEO_URLSUFFIX="${CVMFS_UPDATEGEO_URLSUFFIX:-/${CVMFS_UPDATEGEO_DB}.gz}"
elif [ "$CVMFS_UPDATEGEO_SOURCE" = "maxmind" ]; then
  CVMFS_UPDATEGEO_MINDAYS=${CVMFS_UPDATEGEO_MINDAYS:-14}
  CVMFS_UPDATEGEO_MAXDAYS=${CVMFS_UPDATEGEO_MAXDAYS:-28}
  CVMFS_UPDATEGEO_OLDDB=iplocation.mmdb
  CVMFS_UPDATEGEO_DB="${CVMFS_UPDATEGEO_DB:-GeoLite2-City.mmdb}"
  CVMFS_UPDATEGEO_URLBASE="${CVMFS_UPDATEGEO_URLBASE:-https://download.maxmind.com/geoip/databases/GeoLite2-City/download}"
  CVMFS_UPDATEGEO_URLSUFFIX="${CVMFS_UPDATEGEO_URLSUFFIX:-?suffix=tar.gz}"
elif [ -n "$CVMFS_UPDATEGEO_SOURCE" ] && [ "$CVMFS_UPDATEGEO_SOURCE" != "none" ]; then
  die "CVMFS_UPDATEGEO_SOURCE not openhtc, maxmind, or none"
fi

# Path to some useful sbin utilities
LSOF_BIN="$(find_sbin       lsof)"       || true
GETENFORCE_BIN="$(find_sbin getenforce)" || true
SESTATUS_BIN="$(find_sbin   sestatus)"   || true
GETCAP_BIN="$(find_sbin     getcap)"     || true
SETCAP_BIN="$(find_sbin     setcap)"     || true
MODPROBE_BIN="$(find_sbin   modprobe)"   || true
PIDOF_BIN="$(find_sbin      pidof)"      || true
RUNUSER_BIN="$(find_sbin    runuser)"    || true

# Find out how to deal with Apache
# (binary name, configuration directory, CLI, WSGI module name, ...)
if find_sbin httpd2 > /dev/null 2>&1; then # SLES/OpenSuSE
  APACHE_CONF="apache2"
  APACHE_BIN="$(find_sbin httpd2)"
  APACHE_CTL="$APACHE_BIN"
  APACHE_WSGI_MODPKG="apache2-mod_wsgi"
elif find_sbin apache2 > /dev/null 2>&1; then
  APACHE_CONF="apache2"
  APACHE_BIN="$(find_sbin apache2)"
  if find_sbin apachectl > /dev/null 2>&1; then # Debian
    APACHE_CTL="$(find_sbin apachectl)"
    APACHE_WSGI_MODPKG="libapache2-mod-wsgi"
  elif find_sbin apache2ctl > /dev/null 2>&1; then # Gentoo
    APACHE_CTL="$(find_sbin apache2ctl)"
    APACHE_WSGI_MODPKG="www-apache/mod_wsgi"
  fi
else # RedHat based
  APACHE_CONF="httpd"
  APACHE_BIN="/usr/sbin/httpd"
  APACHE_CTL="$APACHE_BIN"
  APACHE_WSGI_MODPKG="mod_wsgi"
fi

SUPERVISOR_BIN="false"
if [ -f /bin/supervisorctl ]; then
  SUPERVISOR_BIN=/bin/supervisorctl
fi
SERVICE_BIN="false"
if [ ! -f /bin/systemctl ]; then
  if cvmfs_sys_file_is_executable /sbin/service ; then
    SERVICE_BIN="/sbin/service"
  elif cvmfs_sys_file_is_executable /usr/sbin/service ; then
    SERVICE_BIN="/usr/sbin/service" # Ubuntu
  elif cvmfs_sys_file_is_executable /sbin/rc-service ; then
    SERVICE_BIN="/sbin/rc-service" # OpenRC
  else
    die "Neither systemd nor service binary detected"
  fi
fi

# Check if `runuser` is available on this system
# Note: at least Ubuntu in older versions doesn't provide this command
HAS_RUNUSER=0
if [ "x$RUNUSER_BIN" != "x" ]; then
  HAS_RUNUSER=1
fi

# standard values
CVMFS_DEFAULT_GENERATE_LEGACY_BULK_CHUNKS=false
CVMFS_DEFAULT_USE_FILE_CHUNKING=true
CVMFS_DEFAULT_MIN_CHUNK_SIZE=4194304
CVMFS_DEFAULT_AVG_CHUNK_SIZE=8388608
CVMFS_DEFAULT_MAX_CHUNK_SIZE=16777216
CVMFS_DEFAULT_ENFORCE_LIMITS=false
CVMFS_DEFAULT_AUTO_GC_LAPSE='1 day ago'

CVMFS_SERVER_DEBUG=${CVMFS_SERVER_DEBUG:=0}
CVMFS_SERVER_SWISSKNIFE="cvmfs_swissknife"
CVMFS_SERVER_SWISSKNIFE_DEBUG=$CVMFS_SERVER_SWISSKNIFE
# cvmfs_publish will eventually become cvmfs_server, removing the shell wrapper
CVMFS_SERVER_PUBLISH="/usr/bin/cvmfs_publish"
CVMFS_SERVER_PUBLISH_DEBUG=$CVMFS_SERVER_PUBLISH

# On newer Apache version, reloading is asynchonrous and not guaranteed to succeed.
# The integration test cases set this parameter to true.
CVMFS_SERVER_APACHE_RELOAD_IS_RESTART=${CVMFS_SERVER_APACHE_RELOAD_IS_RESTART:=false}

################################################################################
#                                                                              #
#                              Utility Functions                               #
#                                                                              #
################################################################################

# enable the debug mode?
if [ $CVMFS_SERVER_DEBUG -ne 0 ]; then
  if cvmfs_sys_file_is_regular /usr/bin/cvmfs_swissknife_debug ; then
    case $CVMFS_SERVER_DEBUG in
      1)
        # in case something breaks we are provided with a GDB prompt.
        CVMFS_SERVER_SWISSKNIFE_DEBUG="gdb --quiet --eval-command=run --eval-command=quit --args cvmfs_swissknife_debug"
      ;;
      2)
        # attach gdb and provide a prompt WITHOUT actual running the program
        CVMFS_SERVER_SWISSKNIFE_DEBUG="gdb --quiet --args cvmfs_swissknife_debug"
      ;;
      3)
        # do not attach gdb just run debug version
        CVMFS_SERVER_SWISSKNIFE_DEBUG="cvmfs_swissknife_debug"
      ;;
    esac
  else
    echo -e "WARNING: compile with CVMFS_SERVER_DEBUG to allow for debug mode!\nFalling back to release mode [cvmfs_swissknife]...."
  fi

  if cvmfs_sys_file_is_regular /usr/bin/cvmfs_publish_debug ; then
    case $CVMFS_SERVER_DEBUG in
      1)
        # in case something breaks we are provided with a GDB prompt.
        CVMFS_SERVER_PUBLISH_DEBUG="gdb --quiet --eval-command=run --eval-command=quit --args /usr/bin/cvmfs_publish_debug"
      ;;
      2)
        # attach gdb and provide a prompt WITHOUT actual running the program
        CVMFS_SERVER_PUBLISH_DEBUG="gdb --quiet --args /usr/bin/cvmfs_publish_debug"
      ;;
      3)
        # do not attach gdb just run debug version
        CVMFS_SERVER_PUBLISH_DEBUG="/usr/bin/cvmfs_publish_debug"
      ;;
    esac
  else
    echo -e "WARNING: compile with CVMFS_SERVER_DEBUG to allow for debug mode!\nFalling back to release mode [cvmfs_publish]...."
  fi
fi

APACHE_CONF_MODE_CONFD=1     # *.conf goes to ${APACHE_CONF}/conf.d
APACHE_CONF_MODE_CONFAVAIL=2 # *.conf goes to ${APACHE_CONF}/conf-available


################################################################################
#                                                                              #
#                                Entry Point                                   #
#                                                                              #
################################################################################

# check if there is at least a selected sub-command
if [ $# -lt 1 ] || [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
  usage
fi

## implement --version
if [ "$1" = "--version" ]; then
  echo "CernVM-FS version $(__swissknife --version)"
  exit 0
fi

# check if the given sub-command is known and, if so, call it
subcommand=$1
shift
if is_subcommand $subcommand; then
  # replace a dash (-) by an underscore (_) and call the requested sub-command
  # preserve spaces and quotes in the parameters: the eval removes the
  #   single quotes here, leaving "$@" for usual shell substitution
  eval "cvmfs_server_$(echo $subcommand | sed 's/-/_/g')" '"$@"'
else
  usage "Unrecognized command: $subcommand"
fi
