#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server add-replica" command

# This file depends on fuctions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh

cvmfs_server_add_replica() {
  local name
  local alias_name
  local stratum0
  local stratum1_url
  local public_key
  local upstream
  local owner
  local silence_httpd_warning=0
  local configure_apache=1
  local enable_auto_gc=0
  local s3_config

  # optional parameter handling
  OPTIND=1
  while getopts "o:u:n:w:azs:p" option
  do
    case $option in
      u)
        upstream=$OPTARG
      ;;
      o)
        owner=$OPTARG
      ;;
      n)
        alias_name=$OPTARG
      ;;
      w)
        stratum1_url=$OPTARG
      ;;
      a)
        silence_httpd_warning=1
      ;;
      z)
        enable_auto_gc=1
      ;;
      s)
        s3_config=$OPTARG
      ;;
      p)
        configure_apache=0
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command add-replica: Unrecognized option: $1"
      ;;
    esac
  done

   # get stratum0 url and path of public key
  shift $(($OPTIND-1))
  check_parameter_count 2 $#

  stratum0=$1
  public_key=$2

  # get the name of the repository pointed to by $stratum0
  name=$(get_repo_info_from_url $stratum0 -L -n) || die "Failed to access Stratum0 repository at $stratum0"
  name=$(get_repo_info_from_url $stratum0    -n) || die "Failed to access Stratum0 repository at $stratum0"
  if [ x$alias_name = x"" ]; then
    alias_name=$name
  else
    alias_name=$(get_repository_name $alias_name)
  fi

  # sanity checks
  is_valid_repo_name "$alias_name" || die "invalid repository name: $alias_name"
  is_master_replica $stratum0 || die "The repository URL $stratum0 does not point to a replicable master copy of $name"
  if check_repository_existence $alias_name; then
    if is_stratum0 $alias_name; then
      die "Repository $alias_name already exists as a Stratum0 repository.\nUse -n to create an aliased Stratum1 replica for $name on this machine."
    else
      die "There is already a Stratum1 repository $alias_name"
    fi
  fi

  # upstream generation (defaults to local upstream)
  if [ x"$upstream" = x"" ]; then
    if [ x"$s3_config" != x"" ]; then
      upstream=$(make_s3_upstream $alias_name $s3_config)
    else
      upstream=$(make_local_upstream $alias_name)
    fi
  fi

  # stratum1 URL generation (defaults to local URL)
  local stratum1=""
  if [ x"$s3_config" != x"" ]; then
    [ x"$stratum1_url" = x"" ] && die "Please specify the HTTP-URL for S3 (add option -w)"
    stratum1=$(mangle_s3_cvmfs_url $alias_name "$stratum1_url")
  elif [ x"$stratum1_url" = x"" ]; then
    stratum1="$(mangle_local_cvmfs_url $alias_name)"
  else
    stratum1="$stratum1_url"
  fi

  # additional configuration
  local cvmfs_user=$(get_cvmfs_owner $alias_name $owner)
  local spool_dir="/var/spool/cvmfs/${alias_name}"
  local temp_dir="${spool_dir}/tmp"
  local storage_dir=""
  is_local_upstream $upstream && storage_dir=$(get_upstream_config $upstream)

  # additional sanity checks
  is_root || die "Only root can create a new repository"
  check_user $cvmfs_user || die "No user $cvmfs_user"
  check_upstream_validity $upstream
  if is_local_upstream $upstream; then
    if [ $silence_httpd_warning -eq 0 ]; then
      check_apache || die "Apache must be installed and running"
      check_wsgi_module
      if [ x"$cvmfs_user" != x"root" ]; then
        echo "NOTE: If snapshot is not run regularly as root, the GeoIP database will not be updated."
        echo "      You have three options:"
        echo "      1. chown -R $CVMFS_UPDATEGEO_DIR accordingly OR"
        echo "      2. run update-geodb monthly as root OR"
        echo "      3. chown -R $CVMFS_UPDATEGEO_DIR to a dedicated"
        echo "         user ID and run update-geodb monthly as that user"
      fi
    else
      check_apache || echo "Warning: Apache is needed to access this CVMFS replication"
    fi
  fi

  echo -n "Creating Configuration Files... "
  mkdir -p /etc/cvmfs/repositories.d/${alias_name}
  cat > /etc/cvmfs/repositories.d/${alias_name}/server.conf << EOF
# Created by cvmfs_server.
CVMFS_CREATOR_VERSION=$(cvmfs_layout_revision)
CVMFS_REPOSITORY_NAME=$name
CVMFS_REPOSITORY_TYPE=stratum1
CVMFS_USER=$cvmfs_user
CVMFS_SPOOL_DIR=$spool_dir
CVMFS_STRATUM0=$stratum0
CVMFS_STRATUM1=$stratum1
CVMFS_UPSTREAM_STORAGE=$upstream
EOF
  cat > /etc/cvmfs/repositories.d/${alias_name}/replica.conf << EOF
# Created by cvmfs_server.
CVMFS_NUM_WORKERS=16
CVMFS_PUBLIC_KEY=$public_key
CVMFS_HTTP_TIMEOUT=10
CVMFS_HTTP_RETRIES=3
EOF

  # append GC specific configuration
  if [ $enable_auto_gc != 0 ]; then
    cat >> /etc/cvmfs/repositories.d/${alias_name}/server.conf << EOF
CVMFS_AUTO_GC=true
EOF
  fi

  echo "done"

  if is_local_upstream $upstream; then
    _update_geodb -l
    create_global_info_skeleton

    echo -n "Create CernVM-FS Storage... "
    mkdir -p $storage_dir
    create_repository_skeleton $storage_dir $cvmfs_user > /dev/null
    sync
    echo "done"

    if [ $configure_apache -eq 1 ]; then
      echo -n "Update Apache configuration... "
      ensure_enabled_apache_modules
      create_apache_config_for_endpoint $alias_name $storage_dir "with wsgi"
      create_apache_config_for_global_info
      reload_apache > /dev/null
      touch $storage_dir/.cvmfsempty
      wait_for_apache "${stratum1}/.cvmfsempty" || die "fail (Apache configuration)"
      rm -f $storage_dir/.cvmfsempty
      echo "done"
    fi
  fi

  echo -n "Creating CernVM-FS Server Infrastructure... "
  mkdir -p $spool_dir                       || die "fail (mkdir spool)"
  if is_local_upstream $upstream; then
    ln -s ${storage_dir}/data/txn $temp_dir || die "fail (ln -s)"
  else
    mkdir -p $temp_dir                      || die "fail (mkdir temp)"
  fi
  chown -R $cvmfs_user $spool_dir           || die "fail (chown)"
  echo "done"

  echo -n "Updating global JSON information... "
  update_global_repository_info && echo "done" || echo "fail"

  echo "\

Use 'cvmfs_server snapshot' to replicate $alias_name.
Make sure to install the repository public key in /etc/cvmfs/keys/
You might have to add the key in /etc/cvmfs/repositories.d/${alias_name}/replica.conf"
}


