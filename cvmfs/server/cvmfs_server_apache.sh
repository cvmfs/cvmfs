#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Functionality related to the Apache web server

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh


# checks if apache is installed and running
#
# @return  0 if apache is installed and running
check_apache() {
  [ -d /etc/${APACHE_CONF} ] && request_apache_service status > /dev/null
}


request_apache_service() {
  local request_verb="$1"
  if is_systemd; then
    /bin/systemctl $request_verb ${APACHE_CONF}
  elif [ x"$SUPERVISOR_BIN" != x"false" ]; then
    if [ x"$request_verb" = x"reload" ]; then
      request_verb="restart"
    fi
    $SUPERVISOR_BIN $request_verb ${APACHE_CONF}
  else
    $SERVICE_BIN $APACHE_CONF $request_verb
  fi
}


reload_apache() {
  echo -n "Reloading Apache... "
  local verb=reload
  if [ "x$CVMFS_SERVER_APACHE_RELOAD_IS_RESTART" = "xtrue" ]; then
    # The reset-failed verb is only available with systemd
    if is_systemd; then
      request_apache_service reset-failed > /dev/null 2>&1
    fi
    verb=restart
  fi
  request_apache_service $verb > /dev/null || die "fail"
  echo "done"
}


# An Apache reload is asynchronous, the new configuration is not immediately
# accessible.  Wait up to 1 minute until a test url can be fetched
wait_for_apache() {
  local url="$1"

  local now=$(date +%s)
  local deadline=$(($now + 60))
  while [ $now -lt $deadline ]; do
    if curl -f -I --max-time 10 $(get_curl_proxy) $(get_x509_cert_settings) \
       $(get_follow_http_redirects_flag) "$url" >/dev/null 2>&1;
    then
      return 0
    fi
    sleep 1
    now=$(date +%s)
  done

  # timeout
  return 1
}


get_url() {
  local url="$1"
  local timeout="$2"
  local curlopts="$3"

  curl -f -sS --max-time $timeout \
    --retry 2 --retry-delay 5 $curlopts \
    $(get_curl_proxy) $(get_x509_cert_settings) \
    $(get_follow_http_redirects_flag) "$url"
}


check_url() {
  local url="$1"
  local timeout="$2"

  get_url "$1" "$2" -I >/dev/null 2>&1
}


check_apache_module() {
  local module_name="$1"
  ${APACHE_CTL} -M 2>&1 | grep -q "$module_name"
}


# checks if wsgi apache module is installed and enabled
check_wsgi_module() {
  if check_apache_module "wsgi_module"; then
    return 0
  fi

  echo "The apache wsgi module must be installed and enabled.
The required package is called ${APACHE_WSGI_MODPKG}."
  if cvmfs_sys_is_redhat; then
    case "`cat /etc/redhat-release`" in
      *"release 5."*)
        if cvmfs_sys_file_is_regular /etc/httpd/conf.d/wsgi.conf ; then
          # older el5 epel versions didn't automatically enable it
          echo "To enable the module, see instructions in /etc/httpd/conf.d/wsgi.conf"
        else
          echo "The package is in the epel yum repository."
        fi
        ;;
    esac
  fi
  exit 1
}

# checks if proxy apache module is installed and enabled
check_proxy_module() {
  if ! check_apache_module "proxy_module"; then
    echo "The apache proxy module must be installed and enabled."
    exit 1
  fi

  if ! check_apache_module "proxy_http_module"; then
    echo "The apache proxy_http module must be installed and enabled."
    exit 1
  fi

  return 0
}


# retrieves the apache version string
get_apache_version() {
  ${APACHE_BIN} -v | head -n1 | grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+'
}


get_apache_conf_filename() {
  local name=$1
  echo "cvmfs.${name}.conf"
}


restart_apache() {
  echo -n "Restarting Apache... "
  request_apache_service restart > /dev/null || die "fail"
  echo "done"
}


# cvmfs requires a couple of apache modules to be enabled when running on
# an ubuntu machine. This enables these modules on an ubuntu installation
# Note: this function requires a privileged user
ensure_enabled_apache_modules() {
  local a2enmod_bin=
  local apache2ctl_bin=
  a2enmod_bin="$(find_sbin    a2enmod)"    || return 0
  apache2ctl_bin="$(find_sbin apache2ctl)" || return 0

  local restart=0
  local retcode=0
  local modules="headers expires proxy proxy_http"

  for module in $modules; do
    $apache2ctl_bin -M 2>/dev/null | grep -q "$module" && continue
    $a2enmod_bin $module > /dev/null 2>&1 || { echo "Warning: failed to enable apache2 module $module"; retcode=1; }
    restart=1
  done

  # restart apache if needed
  if [ $restart -ne 0 ]; then
    restart_apache 2>/dev/null | { echo "Warning: Failed to restart apache after enabling necessary modules"; retcode=2; }
  fi

  return $retcode
}


# find location of apache configuration files
#
# @return   the location of apache configuration files (stdout)
get_apache_conf_path() {
  local res_path="/etc/${APACHE_CONF}"
  if [ x"$(get_apache_conf_mode)" = x"$APACHE_CONF_MODE_CONFAVAIL" ]; then
    echo "${res_path}/conf-available"
  elif [ -d "${res_path}/modules.d" ]; then
    echo "${res_path}/modules.d"
  else
    echo "${res_path}/conf.d"
  fi
}


# returns the apache configuration string for 'allow from all'
# Note: this is necessary, since apache 2.4.x formulates that different
#
# @return   a configuration snippet to allow s'th from all hosts (stdout)
get_compatible_apache_allow_from_all_config() {
  local minor_apache_version=$(version_minor "$(get_apache_version)")
  if [ $minor_apache_version -ge 4 ]; then
    echo "Require all granted"
  else
    local nl='
'
    echo "Order allow,deny${nl}    Allow from all"
  fi
}


# writes apache configuration file
# This figures out where to put the apache configuration file depending
# on the running apache version
# Note: Configuration file content is expected to come through stdin
#
# @param   file_name  the name of the apache config file (no path!)
# @return             0 on success
create_apache_config_file() {
  local file_name=$1
  local conf_path
  conf_path="$(get_apache_conf_path)"

  # create (or append) the conf file
  cat - > ${conf_path}/${file_name} || return 1

  # the new apache requires the enable the config afterwards
  if [ x"$(get_apache_conf_mode)" = x"$APACHE_CONF_MODE_CONFAVAIL" ]; then
    a2enconf $file_name > /dev/null || return 2
  fi

  return 0
}


# removes apache config files dependent on the apache version in place
# Note: As of apache 2.4.x `a2disconf` needs to be called before removal
#
# @param   file_name  the name of the conf file to be removed (no path!)
# @return  0 on successful removal
remove_apache_config_file() {
  local file_name=$1
  local conf_path
  conf_path="$(get_apache_conf_path)/${file_name}"

  # disable configuration on newer apache versions
  if [ x"$(get_apache_conf_mode)" = x"$APACHE_CONF_MODE_CONFAVAIL" ]; then
    a2disconf $file_name > /dev/null 2>&1 || return 1
  fi

  # remove configuration file
  rm -f $conf_path
}


# check if an apache configuration file exists. This looks in the appropriate
# place, depending on the installed apache version.
has_apache_config_file() {
  local file_name=$1
  is_local_upstream $CVMFS_UPSTREAM_STORAGE || return 1
  local conf_path
  conf_path="$(get_apache_conf_path)/${file_name}"
  cvmfs_sys_file_is_regular $conf_path
}


# figure out apache config file mode
#
# @return   apache config mode (stdout) (see globals below)
get_apache_conf_mode() {
  [ -d /etc/${APACHE_CONF}/conf-available ] && echo $APACHE_CONF_MODE_CONFAVAIL \
                                            || echo $APACHE_CONF_MODE_CONFD
}


# creates a standard Apache configuration file for a repository
#
# @param name         the name of the endpoint to be served
# @param storage_dir  the storage location of the data
# @param with_wsgi    whether or not to enable WSGI api functions
create_apache_config_for_endpoint() {
  local name=$1
  local storage_dir=$2
  local with_wsgi="$3"

  create_apache_config_file "$(get_apache_conf_filename $name)" << EOF
# Created by cvmfs_server.  Don't touch.

KeepAlive On
AddType application/json .json
# Translation URL to real pathname
Alias /cvmfs/${name} ${storage_dir}
<Directory "${storage_dir}">
    Options -MultiViews
    AllowOverride Limit AuthConfig
    $(get_compatible_apache_allow_from_all_config)

    EnableMMAP Off
    EnableSendFile Off

    <FilesMatch "^\.cvmfs">
        ForceType application/x-cvmfs
    </FilesMatch>
    <FilesMatch "^[^.]*$">
        ForceType application/octet-stream
    </FilesMatch>

    # Avoid Last-Modified and If-Modified-Since because of squid bugs
    Header unset Last-Modified
    RequestHeader unset If-Modified-Since
    FileETag None

    ExpiresActive On
    ExpiresDefault "access plus 3 days"
    # 60 seconds and below is not cached at all by Squid default settings
    ExpiresByType application/x-cvmfs "access plus 61 seconds"
    ExpiresByType application/json    "access plus 61 seconds"
</Directory>
EOF

  if [ x"$with_wsgi" != x"" ]; then
    create_apache_config_for_webapi
  fi
}


# creates an Apache proxy-pass configuration file for a pass-through repository
#
# @param name         the name of the endpoint to be served
# @param storage_dir  the storage location of the data
# @param with_wsgi    whether or not to enable WSGI api functions
create_apache_proxy_config_for_endpoint() {
  local name=$1
  local pt_url=$2
  local with_wsgi="$3"

  create_apache_config_file "$(get_apache_conf_filename $name)" << EOF
# Created by cvmfs_server.  Don't touch.

KeepAlive On
AddType application/json .json

# Do not ProxyPass GeoAPI requests
ProxyPass /cvmfs/${name}/api/ !

# ProxyPass data requests
ProxyPass /cvmfs/${name} $pt_url
ProxyPassReverse /cvmfs/${name} $pt_url
EOF

  if [ x"$with_wsgi" != x"" ]; then
    create_apache_config_for_webapi
  fi
}


has_apache_config_for_global_info() {
  has_apache_config_file $(get_apache_conf_filename "info")
}


create_apache_config_for_global_info() {
  ! has_apache_config_for_global_info || return 0
  local storage_dir="${DEFAULT_LOCAL_STORAGE}/info"
  create_apache_config_for_endpoint "info" "$storage_dir"
}


create_apache_config_for_webapi() {
  # start the name with a plus sign to make sure it is read by
  #  apache before the configs for individual repositories
  ! has_apache_config_file $(get_apache_conf_filename +webapi) || return 0
  create_apache_config_file "$(get_apache_conf_filename +webapi)" << EOF
# Created by cvmfs_server.  Don't touch.
AliasMatch ^/cvmfs/([^/]+)/api/(.*)\$ /var/www/wsgi-scripts/cvmfs-server/cvmfs-api.wsgi/\$1/\$2
WSGIDaemonProcess cvmfsapi threads=64 display-name=%{GROUP} \
  python-path=/usr/share/cvmfs-server/webapi
<Directory /var/www/wsgi-scripts/cvmfs-server>
  WSGIProcessGroup cvmfsapi
  WSGIApplicationGroup cvmfsapi
  Options ExecCGI
  SetHandler wsgi-script
  $(get_compatible_apache_allow_from_all_config)
</Directory>
WSGISocketPrefix /var/run/wsgi
EOF
}

# for migrate to 2.1.20 backward compatibility
cat_wsgi_config() {
  # do nothing
  return
}


remove_config_files() {
  local name=$1
  load_repo_config $name

  rm -rf /etc/cvmfs/repositories.d/$name
  local apache_conf_file_name="$(get_apache_conf_filename $name)"
  if has_apache_config_file "$apache_conf_file_name"; then
    remove_apache_config_file "$apache_conf_file_name"
    if [ -z "$(get_or_guess_repository_name)" ]; then
      # no repositories left, remove extra config files
      for confname in +webapi info; do
        apache_conf_file_name="$(get_apache_conf_filename $confname)"
        if has_apache_config_file "$apache_conf_file_name"; then
          remove_apache_config_file "$apache_conf_file_name"
        fi
      done
    fi
    reload_apache > /dev/null
  fi
}


