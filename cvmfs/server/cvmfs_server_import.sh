#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server import" command

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh
# - cvmfs_server_ssl.sh
# - cvmfs_server_apache.sh
# - cvmfs_server_json.sh
# - cvmfs_server_transaction.sh
# - cvmfs_server_publish.sh
# - cvmfs_server_masterkeycard.sh


IMPORT_DESASTER_REPO_NAME=""
IMPORT_DESASTER_MANIFEST_BACKUP=""
IMPORT_DESASTER_MANIFEST_SIGNED=0
_import_desaster_cleanup() {
  local name="$IMPORT_DESASTER_REPO_NAME"
  if [ x"$name" = x"" ]; then
    return 0
  fi

  unmount_and_teardown_repository $name
  remove_spool_area               $name
  remove_config_files             $name

  if [ $IMPORT_DESASTER_MANIFEST_SIGNED -ne 0 ] && \
     [ x$IMPORT_DESASTER_MANIFEST_BACKUP != x"" ]; then
    echo "Manifest was overwritten. If needed here is a backup: $IMPORT_DESASTER_MANIFEST_BACKUP"
  fi
}


# This command needs transaction + publish
migrate_legacy_dirtab() {
  local name=$1
  local dirtab_path="/cvmfs/${name}/.cvmfsdirtab"
  local tmp_dirtab=$(mktemp)

  cp -f "$dirtab_path" "$tmp_dirtab"                           || return 1
  cvmfs_server_transaction $name > /dev/null                   || return 2
  cat "$tmp_dirtab" | sed -e 's/\(.*\)/\1\/\*/' > $dirtab_path || return 3
  cvmfs_server_publish $name > /dev/null                       || return 4
  rm -f "$tmp_dirtab"                                          || return 5
}


cvmfs_server_import() {
  local name
  local stratum0
  local keys_location="/etc/cvmfs/keys"
  local upstream
  local owner
  local file_ownership
  local is_legacy=0
  local show_statistics=0
  local replicable=0
  local chown_backend=0
  local unionfs=
  local recreate_whitelist=0
  local configure_apache=1
  local recreate_repo_key=0
  local require_masterkeycard=0
  local proxy_url

  # parameter handling
  OPTIND=1
  while getopts "w:o:c:u:k:lsmgf:rptRx:" option; do
    case $option in
      w)
        stratum0=$OPTARG
      ;;
      o)
        owner=$OPTARG
      ;;
      c)
        file_ownership=$OPTARG
      ;;
      u)
        upstream=$OPTARG
      ;;
      k)
        keys_location=$OPTARG
      ;;
      l)
        is_legacy=1
      ;;
      s)
        show_statistics=1
      ;;
      m)
        replicable=1
      ;;
      g)
        chown_backend=1
      ;;
      f)
        unionfs=$OPTARG
      ;;
      r)
        recreate_whitelist=1
      ;;
      p)
        configure_apache=0
      ;;
      t)
        recreate_repo_key=1
      ;;
      R)
        recreate_whitelist=1
        require_masterkeycard=1
      ;;
      x)
        proxy_url=$OPTARG
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command import: Unrecognized option: $1"
      ;;
    esac
  done

  # get repository name
  shift $(($OPTIND-1))
  check_parameter_count 1 $#
  name=$(get_repository_name $1)
  is_valid_repo_name "$name" || die "invalid repository name: $name"

  # default values
  [ x"$stratum0" = x ] && stratum0="$(mangle_local_cvmfs_url $name)"
  [ x"$upstream" = x ] && upstream=$(make_local_upstream $name)
  [ x"$unionfs"  = x ] && unionfs="$(get_available_union_fs)"

  local private_key="${name}.key"
  local master_key="${name}.masterkey"
  local certificate="${name}.crt"
  local public_key="${name}.pub"

  # sanity checks
  check_repository_existence $name  && die "The repository $name already exists"
  is_root                           || die "Only root can create a new repository"
  check_upstream_validity $upstream
  check_cvmfs2_client               || die "cvmfs client missing"
  check_autofs_on_cvmfs             && die "Autofs on /cvmfs has to be disabled"
  ensure_swissknife_suid $unionfs   || die "Need CAP_SYS_ADMIN for cvmfs_swissknife"
  lower_hardlink_restrictions
  if [ $configure_apache -eq 1 ]; then
    check_apache                      || die "Apache must be installed and running"
    ensure_enabled_apache_modules
  fi
  [ x"$keys_location" = "x" ] && die "Please provide the location of the repository security keys (-k)"

  if [ $unionfs = "aufs" ]; then
    check_aufs                      || die "aufs kernel module missing"
  fi

  # repository owner dialog
  local cvmfs_user=$(get_cvmfs_owner $name $owner)
  check_user $cvmfs_user || die "No user $cvmfs_user"
  [ x"$file_ownership" = x ] && file_ownership="$(id -u $cvmfs_user):$(id -g $cvmfs_user)"
  echo $file_ownership | grep -q "^[0-9][0-9]*:[0-9][0-9]*$" || die "Unrecognized file ownership: $file_ownership | expected: <uid>:<gid>"
  local cvmfs_uid=$(echo $file_ownership | cut -d: -f1)
  local cvmfs_gid=$(echo $file_ownership | cut -d: -f2)

  # investigate the given repository storage for sanity
  local storage_location=$(get_upstream_config $upstream)
  if is_local_upstream $upstream; then
    local needed_items="${storage_location}                 \
                        ${storage_location}/.cvmfspublished \
                        ${storage_location}/data            \
                        ${storage_location}/data/txn"
    local i=0
    while [ $i -lt 256 ]; do
      needed_items="$needed_items ${storage_location}/data/$(printf "%02x" $i)"
      i=$(($i+1))
    done
    for item in $needed_items; do
      [ -e $item ] || die "$item missing"
      [ $chown_backend -ne 0 ] || [ x"$cvmfs_user" = x"$(stat -c%U $item)" ] || die "$item not owned by $cvmfs_user"
    done
  fi

  # check availability of repository signing key and certificate
  local keys="$public_key"
  if [ $recreate_repo_key -eq 0 ]; then
    if [ ! -f ${keys_location}/${private_key} ] || \
       [ ! -f ${keys_location}/${certificate} ]; then
      die "repository signing key or certificate not found (use -t maybe?)"
    fi
    keys="$keys $private_key $certificate"
  else
    [ $recreate_whitelist -ne 0 ] || die "using -t implies whitelist recreation (use -r maybe?)"
  fi

  # check whitelist expiry date
  if [ $recreate_whitelist -eq 0 ]; then
    cvmfs_sys_file_is_regular "${storage_location}/.cvmfswhitelist" || die "didn't find ${storage_location}/.cvmfswhitelist"
    local expiry=$(get_expiry_from_string "$(cat "${storage_location}/.cvmfswhitelist")")
    [ $expiry -gt 0 ] || die "Repository whitelist expired (use -r maybe?)"
  elif [ $require_masterkeycard -eq 1 ]; then
    local reason
    reason="`masterkeycard_cert_available`" || die "masterkeycard not available to create whitelist: $reason"
  elif ! cvmfs_sys_file_is_regular ${keys_location}/${master_key}; then
    masterkeycard_cert_available >/dev/null || die "Neither masterkey nor masterkeycard found for recreating whitelist"
  fi

  # set up desaster cleanup
  IMPORT_DESASTER_REPO_NAME="$name"
  trap _import_desaster_cleanup EXIT HUP INT QUIT TERM

  # create the configuration for the new repository
  # TODO(jblomer): make a better guess for hash and compression algorithm (see
  # also reflog creation)
  echo -n "Creating configuration files... "
  create_config_files_for_new_repository "$name"             \
                                         "$upstream"         \
                                         "$stratum0"         \
                                         "$cvmfs_user"       \
                                         "$unionfs"          \
                                         "sha1"              \
                                         "true"              \
                                         "false"             \
                                         "$configure_apache" \
                                         "default"           \
                                         "false"             \
                                         ""                  \
                                         ""                  \
                                         "$proxy_url" || die "fail!"
  echo "done"

  # import the old repository security keys
  echo -n "Importing the given key files... "
  if [ $require_masterkeycard -eq 0 ] && \
      cvmfs_sys_file_is_regular ${keys_location}/${master_key} ; then
    keys="$keys $master_key"
  fi
  import_keychain $name "$keys_location" $cvmfs_user "$keys" > /dev/null || die "fail!"
  echo "done"

  # create storage
  echo -n "Creating CernVM-FS Repository Infrastructure... "
  create_spool_area_for_new_repository $name               || die "fail!"
  if [ $configure_apache -eq 1 ]; then
    reload_apache > /dev/null || die "fail!"
  fi
  echo "done"

  # create reflog checksum
  if cvmfs_sys_file_is_regular ${storage_location}/.cvmfsreflog ; then
    echo -n "Re-creating reflog content hash... "
    local reflog_hash=$(cat ${storage_location}/.cvmfsreflog | cvmfs_publish hash -a sha1)
    echo -n $reflog_hash > "${CVMFS_SPOOL_DIR}/reflog.chksum"
    chown $CVMFS_USER "${CVMFS_SPOOL_DIR}/reflog.chksum"
    echo $reflog_hash
  fi

  # load repository configuration file
  load_repo_config $name
  local temp_dir="${CVMFS_SPOOL_DIR}/tmp"

  # import storage location
  if [ $chown_backend -ne 0 ]; then
    echo -n "Importing CernVM-FS storage... "
    chown -R $cvmfs_user $storage_location || die "fail!"
    set_selinux_httpd_context_if_needed $storage_location || die "fail!"
    echo "done"
  fi

  # Let Apache finish reload (needs to happen after SElinux adjustment)
  if [ $configure_apache -eq 1 ]; then
    wait_for_apache "${stratum0}/.cvmfswhitelist" || die "fail (Apache configuration)"
  fi

  # creating a new repository signing key if requested
  if [ $recreate_repo_key -ne 0 ]; then
    echo -n "Creating new repository signing key... "
    local manifest_url="${CVMFS_STRATUM0}/.cvmfspublished"
    local unsigned_manifest="${CVMFS_SPOOL_DIR}/tmp/unsigned_manifest"
    create_cert $name $CVMFS_USER                     || die "fail (certificate creation)!"
    local old_manifest
    old_manifest="`get_item $name $manifest_url`"     || die "fail (manifest download)!"
    echo "$old_manifest" | strip_manifest_signature - > $unsigned_manifest \
                                                      || die "fail (manifest signature strip)!"
    chown $CVMFS_USER $unsigned_manifest              || die "fail (manifest chown)!"
    sign_manifest $name $unsigned_manifest            || die "fail (manifest resign)!"
    echo "done"
  fi

  # recreate whitelist if requested
  if [ $recreate_whitelist -ne 0 ]; then
    create_whitelist $name $CVMFS_USER               \
                           ${CVMFS_UPSTREAM_STORAGE} \
                           ${CVMFS_SPOOL_DIR}/tmp || die "fail!"
  fi

  # migrate old catalogs
  if [ $is_legacy -ne 0 ]; then
    echo "Migrating old catalogs (may take a while)... "
    local new_manifest="${temp_dir}/new_manifest"
    local statistics_flag
    if [ $show_statistics -ne 0 ]; then
      statistics_flag="-s"
    fi
    IMPORT_DESASTER_MANIFEST_BACKUP="${storage_location}/.cvmfspublished.bak"
    cp ${storage_location}/.cvmfspublished \
       $IMPORT_DESASTER_MANIFEST_BACKUP || die "fail! (cannot backup .cvmfspublished)"
    __swissknife migrate               \
      -v "2.0.x"                       \
      -r $storage_location             \
      -n $name                         \
      -u $upstream                     \
      -t $temp_dir                     \
      -k "/etc/cvmfs/keys/$public_key" \
      -o $new_manifest                 \
      -p $cvmfs_uid                    \
      -g $cvmfs_gid                    \
      -f                               \
      $statistics_flag              || die "fail! (migration)"
    chown $cvmfs_user $new_manifest || die "fail! (chown manifest)"

    # sign new (migrated) repository revision
    echo -n "Signing newly imported Repository... "
    local user_shell="$(get_user_shell $name)"
    sign_manifest $name $new_manifest || die "fail! (cannot sign repo)"
    IMPORT_DESASTER_MANIFEST_SIGNED=1
    echo "done"
  fi

  # do final setup
  echo -n "Mounting CernVM-FS Storage... "
  setup_and_mount_new_repository $name || die "fail!"
  echo "done"

  # the .cvmfsdirtab semantics might need an update
  if [ $is_legacy -ne 0 ] && cvmfs_sys_file_is_regular /cvmfs/${name}/.cvmfsdirtab ; then
    echo -n "Migrating .cvmfsdirtab... "
    migrate_legacy_dirtab $name || die "fail!"
    echo "done"
  fi

  # make stratum0 repository replicable if requested
  if [ $replicable -eq 1 ]; then
    cvmfs_server_alterfs -m on $name
  fi

  echo -n "Updating global JSON information... "
  update_global_repository_info && echo "done" || echo "fail"

  # reset trap and finish
  trap - EXIT HUP INT QUIT TERM
  print_new_repository_notice $name $cvmfs_user 1

  # print warning if OverlayFS is used for repository management
  if [ x"$CVMFS_UNION_FS_TYPE" = x"overlayfs" ]; then
    echo ""
    echo "WARNING: You are using OverlayFS which cannot handle hard links."
    echo "         If the imported repository '${name}' used to be based on"
    echo "         AUFS, please run the following command NOW to remove hard"
    echo "         links from the catalogs:"
    echo ""
    echo "    cvmfs_server eliminate-hardlinks ${name}"
    echo ""
  fi
}


