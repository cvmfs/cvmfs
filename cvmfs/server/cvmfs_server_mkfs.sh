#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server mkfs" command

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh
# - cvmfs_server_ssl.sh
# - cvmfs_server_apache.sh
# - cvmfs_server_json.sh
# - cvmfs_server_transaction.sh
# - cvmfs_server_publish.sh
# - cvmfs_server_masterkeycard.sh

cvmfs_server_alterfs() {
  local master_replica=-1
  local name

  # parameter handling
  OPTIND=1
  while getopts "m:" option; do
    case $option in
      m)
        if [ x$OPTARG = "xon" ]; then
          master_replica=1
        elif [ x$OPTARG = "xoff" ]; then
          master_replica=0
        else
          usage "Command alterfs: parameter -m expects 'on' or 'off'"
        fi
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command alterfs: Unrecognized option: $1"
      ;;
    esac
  done

  # get repository name
  shift $(($OPTIND-1))
  check_parameter_count_with_guessing $#
  name=$(get_or_guess_repository_name $1)

  # sanity checks
  check_repository_existence $name || die "The repository $name does not exist"
  [ $master_replica -ne -1 ] || usage "Command alterfs: What should I change?"
  is_root || die "Only root can alter a repository"

  # gather repository information
  load_repo_config $name
  local temp_dir="${CVMFS_SPOOL_DIR}/tmp"

  # do what you've been asked for
  local success=1
  if is_master_replica $name && [ $master_replica -eq 0 ]; then
    echo -n "Disallowing Replication of this Repository... "
    __swissknife remove -o ".cvmfs_master_replica" -r $CVMFS_UPSTREAM_STORAGE > /dev/null || success=0
    if [ $success -ne 1 ]; then
      echo "fail!"
      return 1
    else
      echo "done"
    fi
  elif ! is_master_replica $name && [ $master_replica -eq 1 ]; then
    echo -n "Allowing Replication of this Repository... "
    local master_replica="${temp_dir}/.cvmfs_master_replica"
    # Azurite does not like direct empty file uploads;
    echo "This file marks the repository as replicatable to stratum 1 servers" > $master_replica
    __swissknife upload -i $master_replica -o $(basename $master_replica) -r $CVMFS_UPSTREAM_STORAGE > /dev/null || success=0
    if [ $success -ne 1 ]; then
      echo "fail!"
      return 1
    else
      echo "done"
    fi
    rm -f $master_replica
  fi
}


################################################################################


cvmfs_server_mkfs() {
  local name
  local stratum0
  local upstream
  local owner
  local replicable=1
  local volatile_content=0
  local autotagging=true
  local auto_tag_timespan=
  local unionfs
  local hash_algo
  local compression_alg
  local garbage_collectable=false
  local s3_config=""
  local keys_import_location
  local external_data=false
  local require_masterkeycard=0
  local ignore_manifest_overwrite=0

  local configure_apache=1
  local voms_authz=""
  local proxy_url

  # parameter handling
  OPTIND=1
  while getopts "Xw:u:o:mf:vgG:a:zs:k:pRV:Z:x:I" option; do
    case $option in
      X)
        external_data=true
      ;;
      w)
        stratum0=$OPTARG
      ;;
      u)
        upstream=$OPTARG
      ;;
      o)
        owner=$OPTARG
      ;;
      m)
        replicable=1
      ;;
      f)
        unionfs=$OPTARG
      ;;
      v)
        volatile_content=1
      ;;
      g)
        autotagging=false
      ;;
      G)
        auto_tag_timespan="$OPTARG"
      ;;
      a)
        hash_algo=$OPTARG
      ;;
      z)
        garbage_collectable=true
      ;;
      s)
        s3_config=$OPTARG
      ;;
      k)
        keys_import_location=$OPTARG
      ;;
      R)
        require_masterkeycard=1
      ;;
      Z)
        compression_alg=$OPTARG
      ;;
      p)
        configure_apache=0
      ;;
      V)
        voms_authz=$OPTARG
      ;;
      x)
        proxy_url=$OPTARG
      ;;
      I)
        ignore_manifest_overwrite=1
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command mkfs: Unrecognized option: $1"
      ;;
    esac
  done

  # get repository name
  shift $(($OPTIND-1))
  check_parameter_count 1 $#
  name=$(get_repository_name $1)

  is_valid_repo_name "$name" || die "invalid repository name: $name"
  is_root                    || die "Only root can create a new repository"

  # default values
  [ x"$unionfs"   = x"" ] && unionfs="$(get_available_union_fs)"
  [ x"$hash_algo" = x"" ] && hash_algo=sha1
  [ x"$compression_alg" = x"" ] && compression_alg=default

  # upstream generation (defaults to local upstream)
  if [ x"$upstream" = x"" ]; then
    if [ x"$s3_config" != x"" ]; then
      local subpath=$(parse_url $stratum0 path)
      upstream=$(make_s3_upstream $name $s3_config $subpath)
    else
      upstream=$(make_local_upstream $name)
    fi
  fi

  # stratum0 URL generation (defaults to local URL)
  if [ x"$s3_config" != x"" ]; then
    [ x"$stratum0" = x"" ] && die "Please specify the HTTP-URL for S3 (add option -w)"
    stratum0=$(mangle_s3_cvmfs_url $name "$stratum0")
  elif [ x"$stratum0" = x"" ]; then
    stratum0="$(mangle_local_cvmfs_url $name)"
  fi

  # sanity checks
  local upstream_type=$(get_upstream_type $upstream)
  check_repository_existence $name  && die "The repository $name already exists"
  # if upstream is a gateway, we expect the repository to not be empty.
  if [ x"$upstream_type" != xgw ]; then
      if [ $ignore_manifest_overwrite -eq 0 ]; then
         is_empty_repository_from_url $stratum0 ||
             die "Error: A manifest already exists at this url: $stratum0/.cvmfspublished .\n
                 Delete it manually or use cvmfs_server  mkfs -I  to force the creation of a new repository in this non-empty
                 storage. \n\n If you meant to setup a new stratum0 for this existing repository, use cvmfs_server import."
      fi
  fi
  check_upstream_validity $upstream
  if [ $unionfs = "aufs" ]; then
    check_aufs                      || die "aufs kernel module missing"
  fi
  check_cvmfs2_client               || die "cvmfs client missing"
  check_autofs_on_cvmfs             && die "Autofs on /cvmfs has to be disabled"
  lower_hardlink_restrictions
  ensure_swissknife_suid $unionfs   || die "Need CAP_SYS_ADMIN for cvmfs_swissknife"
  if is_local_upstream $upstream; then
    check_apache                    || die "Apache must be installed and running"
    ensure_enabled_apache_modules
  fi
  if [ "x$auto_tag_timespan" != "x" ]; then
    date --date "$auto_tag_timespan" +%s >/dev/null 2>&1 || die "Auto tags time span cannot be parsed"
    [ x"$autotagging" = x"false" ] &&
      echo "Warning: auto tags time span set but auto tagging turned off" || true
  fi

  # check if the keychain for the repository to create is already in place
  local keys_location="/etc/cvmfs/keys"
  mkdir -p $keys_location
  local keys="${name}.crt ${name}.pub"
  if [ x"$upstream_type" = xgw ]; then
      keys="$keys ${name}.gw"
  else
      keys="$keys ${name}.key"
  fi
  if [ $require_masterkeycard -eq 1 ]; then
      local reason
      reason="`masterkeycard_cert_available`" || die "masterkeycard not available: $reason"
  elif masterkeycard_cert_available >/dev/null; then
      require_masterkeycard=1
  else
      if [ x"$upstream_type" != xgw ]; then
          keys="${name}.masterkey $keys"
      fi
  fi
  local keys_are_there=0
  for k in $keys; do
    if cvmfs_sys_file_is_regular "${keys_location}/${k}"; then
      keys_are_there=1
      break
    fi
  done
  if [ $keys_are_there -eq 1 ]; then
    # just import the keys that are already there if they do not overwrite existing keys
    if [ x"$keys_import_location" != x""               ] && \
       [ x"$keys_import_location" != x"$keys_location" ]; then
      die "Importing keys from '$keys_import_location' would overwrite keys in '$keys_location'"
    fi
    keys_import_location=$keys_location
  fi

  # repository owner dialog
  local cvmfs_user=$(get_cvmfs_owner $name $owner)
  check_user $cvmfs_user || die "No user $cvmfs_user"

  # GC and auto-tag warning
  if [ x"$autotagging" = x"true" ] && [ x"$auto_tag_timespan" = "x" ] && [ x"$garbage_collectable" = x"true" ]; then
    echo "Note: Autotagging all revisions impedes garbage collection"
  fi

  # create system-wide configuration
  echo -n "Creating Configuration Files... "
  create_config_files_for_new_repository "$name"                \
                                         "$upstream"            \
                                         "$stratum0"            \
                                         "$cvmfs_user"          \
                                         "$unionfs"             \
                                         "$hash_algo"           \
                                         "$autotagging"         \
                                         "$garbage_collectable" \
                                         "$configure_apache"    \
                                         "$compression_alg"     \
                                         "$external_data"       \
                                         "$voms_authz"          \
                                         "$auto_tag_timespan"   \
                                         "$proxy_url" || die "fail"
  echo "done"

  # create or import security keys and certificates
  if [ x"$keys_import_location" = x"" ]; then
    echo -n "Creating CernVM-FS Master Key and Self-Signed Certificate... "
    create_master_key $name $cvmfs_user || die "fail (master key)"
    create_cert $name $cvmfs_user       || die "fail (certificate)"
    echo "done"
  else
    echo -n "Importing CernVM-FS Master Key and Certificate from '$keys_import_location'... "
    import_keychain $name "$keys_import_location" $cvmfs_user "$keys" > /dev/null || die "fail!"
    echo "done"
  fi

  # create spool area and mountpoints
  echo -n "Creating CernVM-FS Server Infrastructure... "
  create_spool_area_for_new_repository $name || die "fail"
  echo "done"

  # create storage area
  if is_local_upstream $upstream; then
    echo -n "Creating Backend Storage... "
    create_global_info_skeleton     || die "fail"
    create_repository_storage $name || die "fail"
    echo "done"
  fi

  # get information about new repository
  load_repo_config $name
  local temp_dir="${CVMFS_SPOOL_DIR}/tmp"
  local rdonly_dir="${CVMFS_SPOOL_DIR}/rdonly"
  local scratch_dir="${CVMFS_SPOOL_DIR}/scratch/current"

  # create the whitelist
  if [ x"$upstream_type" != xgw ]; then
      create_whitelist $name $cvmfs_user $upstream $temp_dir
  fi

  echo -n "Creating Initial Repository... "
  local repoinfo_file=${temp_dir}/new_repoinfo
  touch $repoinfo_file
  create_repometa_skeleton $repoinfo_file
  if is_local_upstream $upstream && [ $configure_apache -eq 1 ]; then
    reload_apache > /dev/null
    wait_for_apache "${stratum0}/.cvmfswhitelist" || die "fail (Apache configuration)"
  fi

  local volatile_opt=
  if [ $volatile_content -eq 1 ]; then
    volatile_opt="-v"
    echo -n "(repository flagged volatile)... "
  fi
  local user_shell="$(get_user_shell $name)"
  if [ x"$upstream_type" != xgw ]; then
      local create_cmd="$(__swissknife_cmd) create  \
      -t $temp_dir                                \
      -r $upstream                                \
      -n $name                                    \
      -a $hash_algo $volatile_opt                 \
      -o ${temp_dir}/new_manifest                 \
      -R $(get_reflog_checksum $name)"
      if $garbage_collectable; then
          create_cmd="$create_cmd -z"
      fi
      if [ "x$voms_authz" != "x" ]; then
          echo -n "(repository will be accessible with VOMS credentials $voms_authz)... "
          create_cmd="$create_cmd -V $voms_authz"
      fi

      $user_shell "$create_cmd" > /dev/null                       || die "fail! (cannot init repo)"
      sign_manifest $name ${temp_dir}/new_manifest $repoinfo_file || die "fail! (cannot sign repo)"
  fi
  echo "done"

  echo -n "Mounting CernVM-FS Storage... "
  setup_and_mount_new_repository $name || die "fail"
  echo "done"

  if [ $replicable -eq 1 ]; then
    cvmfs_server_alterfs -m on $name
  fi

  health_check $name || die "fail! (health check after mount)"

  if [ x"$upstream_type" != xgw -a "x$voms_authz" = "x" ]; then
      echo -n "Initial commit... "
      cvmfs_server_transaction $name > /dev/null || die "fail (transaction)"
      echo "New CernVM-FS repository for $name" > /cvmfs/${name}/new_repository
      chown $cvmfs_user /cvmfs/${name}/new_repository
      cvmfs_server_publish $name > /dev/null || die "fail (publish)"
      # When publishing an external repository, it is the user's responsibility to
      # stage the actual data files to the web server - not the publication function.
      # Hence, the following is guaranteed to not work.
      if [ $external_data = "false" ]; then
          cat $rdonly_dir/new_repository || die "fail (finish)"
      fi
  fi

  echo -n "Updating global JSON information... "
  update_global_repository_info && echo "done" || echo "fail"

  syncfs

  print_new_repository_notice $name $cvmfs_user $require_masterkeycard
}


