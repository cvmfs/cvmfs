#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Functionality related to the Apache web server

# This file depends on fuctions implemented in the following files:
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
  else
    $SERVICE_BIN $APACHE_CONF $request_verb
  fi
}


reload_apache() {
  echo -n "Reloading Apache... "
  request_apache_service reload > /dev/null || die "fail"
  echo "done"
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
  if is_redhat; then
    case "`cat /etc/redhat-release`" in
      *"release 5."*)
        if [ -f /etc/httpd/conf.d/wsgi.conf ]; then
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
  local modules="headers expires"

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
# @return             0 on succes
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
  local conf_path
  conf_path="$(get_apache_conf_path)/${file_name}"
  [ -f $conf_path ]
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
$(cat_wsgi_config $name $with_wsgi)

KeepAlive On
AddType application/json .json
# Translation URL to real pathname
Alias /cvmfs/${name} ${storage_dir}
<Directory "${storage_dir}">
    Options -MultiViews
    AllowOverride Limit
    $(get_compatible_apache_allow_from_all_config)

    EnableMMAP Off
    EnableSendFile Off

    <FilesMatch "^\.cvmfs">
        ForceType application/x-cvmfs
    </FilesMatch>

    Header unset Last-Modified
    FileETag None

    ExpiresActive On
    ExpiresDefault "access plus 3 days"
    ExpiresByType application/x-cvmfs "access plus 2 minutes"
    ExpiresByType application/json    "access plus 2 minutes"
</Directory>
EOF
}


has_apache_config_for_global_info() {
  has_apache_config_file $(get_apache_conf_filename "info")
}


create_apache_config_for_global_info() {
  ! has_apache_config_for_global_info || return 0
  local storage_dir="${DEFAULT_LOCAL_STORAGE}/info"
  create_apache_config_for_endpoint "info" "$storage_dir"
}


# sends wsgi configuration to stdout
#
# @param name        the name of the repository
# @param with_wsgi   if not set, this function is a NOOP
cat_wsgi_config() {
  local name=$1
  local with_wsgi="$2"

  [ x"$with_wsgi" != x"" ] || return 0
  echo "# Enable api functions
WSGIPythonPath /usr/share/cvmfs-server/webapi
Alias /cvmfs/$name/api /var/www/wsgi-scripts/cvmfs-api.wsgi/$name

<Directory /var/www/wsgi-scripts>
    Options ExecCGI
    SetHandler wsgi-script
    Order allow,deny
    Allow from all
</Directory>
"
}


remove_config_files() {
  local name=$1
  load_repo_config $name

  local apache_conf_file_name="$(get_apache_conf_filename $name)"
  if is_local_upstream $CVMFS_UPSTREAM_STORAGE &&
     has_apache_config_file "$apache_conf_file_name"; then
    remove_apache_config_file "$apache_conf_file_name"
    reload_apache > /dev/null
  fi
  rm -rf /etc/cvmfs/repositories.d/$name
}


