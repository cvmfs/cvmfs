#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server portal" command

# This file depends on fuctions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh


__portal_check_system_requirements() {
  is_systemd || { echo "Portals require a systemd managed machine" 2>&1; return 1; }
  which cvmfs_minio > /dev/null 2>&1 || \
    { echo "The cvmfs_minio binary is missing. Is the cvmfs-server-portals package installed?" 2>&1; return 1; }
  [ -f $CVMFS_PORTAL_SYSTEMD_TEMPLATE ] || \
    { echo "The systemd template $CVMFS_PORTAL_SYSTEMD_TEMPLATE is missing. Is the cvmfs-server-portals package installed?" 2>&1; return 1; }
}


__portal_get_config_dir() {
  local reponame="$1"
  local portalname="$2"
  echo "/etc/cvmfs/repositories.d/$reponame/portals/$portalname"
}

__portal_get_spool_dir() {
  local reponame="$1"
  local portalname="$2"
  echo "/var/spool/cvmfs/$reponame/portals/$portalname"
}

__portal_get_systemd_link() {
  local reponame="$1"
  local portalname="$2"
  echo "/etc/cvmfs/repositories.d/${reponame}:${portalname}"
}


__portal_get_systemd_unit() {
  local reponame="$1"
  local portalname="$2"
  echo "cvmfs-portal@${reponame}:${portalname}.service"
}


__portal_exists() {
  local reponame="$1"
  local portalname="$2"

  [ -d $(__portal_get_config_dir $reponame $portalname) ] && return 0
  return 1
}


__portal_list() {
  local reponame="$1"

  for portal in /etc/cvmfs/repositories.d/$reponame/portals/*; do
    if [ "x$portal" = "x/etc/cvmfs/repositories.d/$reponame/portals/*" ]; then
      return 0
    fi

    local name=$(basename $portal)
    echo "Portal $name"
    # Todo: running (on port), base path
  done
}


__portal_find_free_port() {
  local max_port=9000
  # There must be at least one repo
  for repo in /etc/cvmfs/repositories.d/*; do
    for portal in $repo/portals/*; do
      if [ "x$portal" = "x$repo/portals/*" ]; then
        continue
      fi
      [ -f $portal/env.conf ] && . $portal/env.conf
      if [ "x$CVMFS_PORTAL_PORT" != "x" ]; then
        [ $CVMFS_PORTAL_PORT -ge $max_port ] && max_port=$(($CVMFS_PORTAL_PORT + 1))
      fi
    done
  done
  echo $max_port
}


__portal_add() {
  local reponame="$1"
  local portalname="$2"
  load_repo_config $name

  # sanity checks
  # TODO(jblomer): sanitize portal name
  __portal_exists $reponame $portalname && \
    { echo "Portal $reponame:$portalname exists already" >&2; return 1; } || true
  __portal_check_system_requirements || return 1
  is_root || { echo "Only root can add a portal" >&2; return 1; }
  check_apache || { echo "Apache must be installed and running" >&2; return 1; }

  local access_key=$(cat /dev/urandom | tr -cd A-Z0-9 | head -c 20)
  local secret_key=$(cat /dev/urandom | tr -cd a-zA-Z0-9 | head -c 40)
  local port=$(__portal_find_free_port)
  local config_dir="$(__portal_get_config_dir $reponame $portalname)"
  local spool_dir="$(__portal_get_spool_dir $reponame $portalname)"
  local systemd_link="$(__portal_get_systemd_link $reponame $portalname)"
  local systemd_unit="$(__portal_get_systemd_unit $reponame $portalname)"
  local user_shell="$(get_user_shell $name)"

  mkdir -p $config_dir/certs/CAs
  cat >$config_dir/env.conf << EOF
CVMFS_PORTAL_USER=$CVMFS_USER
CVMFS_PORTAL_PORT=$port
CVMFS_PORTAL_SPOOL_DIR=$spool_dir
EOF
  cat >$config_dir/config.json << EOF
{
  "version": "19",
  "credential": {
    "accessKey": "$access_key",
    "secretKey": "$secret_key"
  },
  "region": "",
  "browser": "off",
  "logger": {
    "console": {
      "enable": true
    }
  },
  "notify": {
    "webhook": {
      "1": {
        "enable": false,
        "endpoint": ""
      }
    }
  }
}
EOF
  chmod 0600 $config_dir/config.json
  chown $CVMFS_USER $config_dir/config.json

  $user_shell "mkdir -p $spool_dir"

  ln -s $config_dir $systemd_link
  systemctl enable -q $systemd_unit
  systemctl start $systemd_unit

  ensure_enabled_apache_modules proxy
  create_apache_config_for_portal $reponame $portalname $port
  reload_apache
}


__portal_rm() {
  local reponame="$1"
  local portalname="$2"
  load_repo_config $name

  __portal_exists $reponame $portalname || \
    { echo "Portal $reponame:$portalname does not exist" >&2; return 1; }
  is_root || { echo "Only root can remove a portal" >&2; return 1; }

  remove_apache_config_file $(portal_get_apache_conf $reponame $portalname)
  reload_apache

  local systemd_unit="$(__portal_get_systemd_unit $reponame $portalname)"
  systemctl stop $systemd_unit || true
  systemctl disable -q $systemd_unit || true
  rm -f /etc/systemd/system/$systemd_unit
  systemctl daemon-reload
  rm -f "$(__portal_get_systemd_link $reponame $portalname)"

  local spool_dir="$(__portal_get_spool_dir $reponame $portalname)"
  rm -rf $spool_dir

  local config_dir="$(__portal_get_config_dir $reponame $portalname)"
  rm -rf $config_dir
}


cvmfs_server_portal() {
  local do_list=0
  local portal_add
  local portal_rm

  OPTIND=1
  while getopts "a:r:l" option; do
    case $option in
      a)
        portal_add="$OPTARG"
      ;;
      r)
        portal_rm="$OPTARG"
      ;;
      l)
        do_list=1
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command catalog-chown: Unrecognized option: $1"
      ;;
    esac
  done
  shift $(($OPTIND-1))

   # get repository names
  check_parameter_count_for_multiple_repositories $#
  names=$(get_or_guess_multiple_repository_names "$@")

  for name in $names; do
    check_repository_existence $name || \
      die "The repository $name does not exist"
    load_repo_config $name

    check_repository_compatibility $name
    is_stratum0 $name || die "The repository $name is not a stratum 0"

    if [ "x$portal_rm" != "x" ]; then
      __portal_rm $name $portal_rm || return 1
    fi

    if [ "x$portal_add" != "x" ]; then
      __portal_add $name $portal_add || return 1
    fi

    if [ $do_list -eq 1 ]; then
      __portal_list $name
    fi
  done
}


portal_get_apache_conf() {
  local reponame="$1"
  local portalname="$2"
  echo "cvmfs.+portal.${reponame}:${portalname}.conf"
}
