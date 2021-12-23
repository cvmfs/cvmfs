#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server migrate" command

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh


_is_generated_apache_conf() {
  local apache_conf="$1"

  grep -q '^# Created by cvmfs_server.' $apache_conf 2>/dev/null
}

_migrate_2_1_6() {
  local name=$1
  local destination_version="2.1.7"

  # get repository information
  load_repo_config $name

  echo "Migrating repository '$name' from CernVM-FS $(mangle_version_string '2.1.6') to $(mangle_version_string '2.1.7')"

  echo "--> generating new upstream descriptor"
  # before 2.1.6 there were only local backends... no need to differentiate here
  local storage_path=$(echo $CVMFS_UPSTREAM_STORAGE | cut --delimiter=: --fields=2)
  local new_upstream="local,${storage_path}/data/txn,${storage_path}"

  echo "--> removing spooler pipes"
  local pipe_pathes="${CVMFS_SPOOL_DIR}/paths"
  local pipe_digests="${CVMFS_SPOOL_DIR}/digests"
  rm -f $pipe_pathes > /dev/null 2>&1 || echo "Warning: not able to delete $pipe_pathes"
  rm -f $pipe_digests > /dev/null 2>&1 || echo "Warning: not able to delete $pipe_digests"

  if is_stratum0 $name; then
    echo "--> create temp directory in upstream storage"
    local tmp_dir=${storage_path}/data/txn
    mkdir $tmp_dir > /dev/null 2>&1 || echo "Warning: not able to create $tmp_dir"
    chown -R $CVMFS_USER $tmp_dir > /dev/null 2>&1 || echo "Warning: not able to chown $tmp_dir to $CVMFS_USER"
    set_selinux_httpd_context_if_needed $tmp_dir || echo "Warning: not able to chcon $tmp_dir to httpd_sys_content_t"

    echo "--> updating server.conf"
    mv /etc/cvmfs/repositories.d/${name}/server.conf /etc/cvmfs/repositories.d/${name}/server.conf.old
    cat > /etc/cvmfs/repositories.d/${name}/server.conf << EOF
# created by cvmfs_server.
# migrated from version $(mangle_version_string "2.1.6").
CVMFS_CREATOR_VERSION=$destination_version
CVMFS_REPOSITORY_NAME=$CVMFS_REPOSITORY_NAME
CVMFS_REPOSITORY_TYPE=$CVMFS_REPOSITORY_TYPE
CVMFS_USER=$CVMFS_USER
CVMFS_UNION_DIR=$CVMFS_UNION_DIR
CVMFS_SPOOL_DIR=$CVMFS_SPOOL_DIR
CVMFS_STRATUM0=$CVMFS_STRATUM0
CVMFS_UPSTREAM_STORAGE=$new_upstream
CVMFS_GENERATE_LEGACY_BULK_CHUNKS=$CVMFS_DEFAULT_GENERATE_LEGACY_BULK_CHUNKS
CVMFS_USE_FILE_CHUNKING=$CVMFS_DEFAULT_USE_FILE_CHUNKING
CVMFS_MIN_CHUNK_SIZE=$CVMFS_DEFAULT_MIN_CHUNK_SIZE
CVMFS_AVG_CHUNK_SIZE=$CVMFS_DEFAULT_AVG_CHUNK_SIZE
CVMFS_MAX_CHUNK_SIZE=$CVMFS_DEFAULT_MAX_CHUNK_SIZE
EOF
  fi

  if is_stratum1 $name; then
    echo "--> updating server.conf"
    mv /etc/cvmfs/repositories.d/${name}/server.conf /etc/cvmfs/repositories.d/${name}/server.conf.old
    cat > /etc/cvmfs/repositories.d/${name}/server.conf << EOF
# Created by cvmfs_server.
# migrated from version $(mangle_version_string "2.1.6").
CVMFS_CREATOR_VERSION=$destination_version
CVMFS_REPOSITORY_NAME=$CVMFS_REPOSITORY_NAME
CVMFS_REPOSITORY_TYPE=$CVMFS_REPOSITORY_TYPE
CVMFS_USER=$CVMFS_USER
CVMFS_SPOOL_DIR=$CVMFS_SPOOL_DIR
CVMFS_STRATUM0=$CVMFS_STRATUM0
CVMFS_UPSTREAM_STORAGE=$new_upstream
EOF
  fi

  # reload repository information
  load_repo_config $name
}


_migrate_2_1_7() {
  local name=$1
  local destination_version="2.1.15"

  # get repository information
  load_repo_config $name
  local user_shell="$(get_user_shell $name)"

  echo "Migrating repository '$name' from CernVM-FS $CVMFS_CREATOR_VERSION to $(mangle_version_string $destination_version)"

  if [ ! -f ${CVMFS_SPOOL_DIR}/client.local ]; then
    echo "--> creating client.local"
    $user_shell "touch ${CVMFS_SPOOL_DIR}/client.local" || die "fail!"
  fi

  local server_conf="/etc/cvmfs/repositories.d/${name}/server.conf"
  if ! cat $server_conf | grep -q CVMFS_UNION_FS_TYPE; then
    echo "--> setting AUFS as used overlay file system"
    echo "CVMFS_UNION_FS_TYPE=aufs" >> $server_conf
  fi

  if ! grep client.local /etc/fstab | grep -q ${CVMFS_REPOSITORY_NAME}; then
    echo "--> adjusting /etc/fstab"
    sed -i -e "s|cvmfs2#${CVMFS_REPOSITORY_NAME} ${CVMFS_SPOOL_DIR}/rdonly fuse allow_other,config=/etc/cvmfs/repositories.d/${CVMFS_REPOSITORY_NAME}/client.conf,cvmfs_suid 0 0 # added by CernVM-FS for ${CVMFS_REPOSITORY_NAME}|cvmfs2#${CVMFS_REPOSITORY_NAME} ${CVMFS_SPOOL_DIR}/rdonly fuse allow_other,config=/etc/cvmfs/repositories.d/${CVMFS_REPOSITORY_NAME}/client.conf:${CVMFS_SPOOL_DIR}/client.local,cvmfs_suid 0 0 # added by CernVM-FS for ${CVMFS_REPOSITORY_NAME}|" /etc/fstab
    if ! grep client.local /etc/fstab | grep -q ${CVMFS_REPOSITORY_NAME}; then
      die "fail!"
    fi
  fi

  echo "--> analyzing file catalogs for additional statistics counters"
  local temp_dir="${CVMFS_SPOOL_DIR}/tmp"
  local new_manifest="${temp_dir}/new_manifest"

  __swissknife migrate                                 \
    -v "2.1.7"                                         \
    -r ${CVMFS_STRATUM0}                               \
    -n $name                                           \
    -u ${CVMFS_UPSTREAM_STORAGE}                       \
    -t $temp_dir                                       \
    -o $new_manifest                                   \
    -k /etc/cvmfs/keys/$name.pub                       \
    -z /etc/cvmfs/repositories.d/${name}/trusted_certs \
    -s || die "fail! (migrating catalogs)"
  chown ${CVMFS_USER} $new_manifest

  # sign new (migrated) repository revision
  echo -n "Signing newly imported Repository... "
  create_whitelist $name ${CVMFS_USER} ${CVMFS_UPSTREAM_STORAGE} $temp_dir > /dev/null
  sign_manifest $name $new_manifest || die "fail! (cannot sign repo)"
  echo "done"

  echo "--> updating server.conf"
  sed -i -e "s/^CVMFS_CREATOR_VERSION=.*/CVMFS_CREATOR_VERSION=$destination_version/" /etc/cvmfs/repositories.d/$name/server.conf

  # reload (updated) repository information
  load_repo_config $name

  # update repository information
  echo "--> remounting (migrated) repository"
  local remote_hash
  remote_hash=$(get_published_root_hash $name)

  run_suid_helper rw_umount $name     > /dev/null 2>&1 || die "fail! (unmounting /cvmfs/$name)"
  run_suid_helper rdonly_umount $name > /dev/null 2>&1 || die "fail! (unmounting ${CVMFS_SPOOL_DIR}/rdonly)"
  set_ro_root_hash $name $remote_hash
  run_suid_helper rdonly_mount $name  > /dev/null 2>&1 || die "fail! (mounting ${CVMFS_SPOOL_DIR}/$name)"
  run_suid_helper rw_mount $name      > /dev/null 2>&1 || die "fail! (mounting /cvmfs/$name)"
}

# note that this is only run on stratum1s that have local upstream storage
_migrate_2_1_15() {
  local name=$1
  local destination_version="2.1.20"
  local conf_file
  conf_file="$(get_apache_conf_path)/$(get_apache_conf_filename $name)"

  # get repository information
  load_repo_config $name

  echo "Migrating repository '$name' from CernVM-FS $CVMFS_CREATOR_VERSION to $(mangle_version_string $destination_version)"

  if check_apache; then
    check_wsgi_module
    _update_geodb -l
  fi
  # else apache is currently stopped, add-replica may have been run with -a

  if cvmfs_sys_file_is_regular "$conf_file" ; then
    echo "--> updating $conf_file"
    (echo "# Created by cvmfs_server.  Don't touch."
     cat_wsgi_config $name
     sed '/^# Created by cvmfs_server/d' $conf_file) > $conf_file.NEW
    cat $conf_file.NEW >$conf_file
    rm -f $conf_file.NEW
    if check_apache; then
      # Need to restart, reload doesn't work at least for the first module;
      #  that results in repeated segmentation faults on RHEL5 & 6
      restart_apache
    fi
  else
    if check_apache; then
      echo "$conf_file does not exist."
      echo "Make sure the equivalent of the following is in the apache configuration:"
      echo ----------
    else
      echo "Apache is not enabled and $conf_file does not exist."
      echo "  If you do enable Apache, make sure the equivalent of the following is"
      echo "  in the apache configuration:"
    fi
    cat_wsgi_config $name
  fi

  echo "--> updating server.conf"
  local server_conf="/etc/cvmfs/repositories.d/${name}/server.conf"
  sed -i -e "s/^CVMFS_CREATOR_VERSION=.*/CVMFS_CREATOR_VERSION=$destination_version/" $server_conf
  echo "CVMFS_STRATUM1=$(mangle_local_cvmfs_url $name)" >> $server_conf

  # reload (updated) repository information
  load_repo_config $name
}

_migrate_2_1_20() {
  local name=$1
  local destination_version="2.2.0-1"
  local creator=$(repository_creator_version $name)
  local server_conf="/etc/cvmfs/repositories.d/${name}/server.conf"
  local apache_conf="$(get_apache_conf_path)/$(get_apache_conf_filename $name)"

  # get repository information
  load_repo_config $name

  echo "Migrating repository '$name' from CernVM-FS $(mangle_version_string $CVMFS_CREATOR_VERSION) to $(mangle_version_string $destination_version)"

  if is_stratum0 $name; then
    echo "--> updating client.conf"
    local client_conf="/etc/cvmfs/repositories.d/${name}/client.conf"
    [ -z "$CVMFS_HIDE_MAGIC_XATTRS" ] && echo "CVMFS_HIDE_MAGIC_XATTRS=yes" >> $client_conf
    [ -z "$CVMFS_FOLLOW_REDIRECTS"  ] && echo "CVMFS_FOLLOW_REDIRECTS=yes"  >> $client_conf
    [ -z "$CVMFS_SERVER_CACHE_MODE" ] && echo "CVMFS_SERVER_CACHE_MODE=yes" >> $client_conf
    [ -z "$CVMFS_MOUNT_DIR"         ] && echo "CVMFS_MOUNT_DIR=/cvmfs"      >> $client_conf

    echo "--> updating /etc/fstab"
    local tmp_fstab=$(mktemp)
    awk  "/added by CernVM-FS for $name\$/"' {
            for (i = 1; i <= NF; i++) {
              if (i == 4) $i = $i",noauto";
              printf("%s ", $i);
            }
            print "";
            next;
          };
          { print $0 }' /etc/fstab > $tmp_fstab
    cat $tmp_fstab > /etc/fstab
    rm -f $tmp_fstab

    echo "--> updating server.conf"
    if ! grep -q "CVMFS_AUTO_REPAIR_MOUNTPOINT" $server_conf; then
      echo "CVMFS_AUTO_REPAIR_MOUNTPOINT=true" >> $server_conf
    else
      sed -i -e "s/^\(CVMFS_AUTO_REPAIR_MOUNTPOINT\)=.*/\1=true/" $server_conf
    fi
  fi

  if is_local_upstream $CVMFS_UPSTREAM_STORAGE && cvmfs_sys_file_is_regular "$apache_conf" ; then
    echo "--> updating apache config ($(basename $apache_conf))"
    local storage_dir=$(get_upstream_config $CVMFS_UPSTREAM_STORAGE)
    local wsgi=""
    is_stratum1 $name && wsgi="enabled"
    create_apache_config_for_endpoint $name $storage_dir $wsgi
    reload_apache > /dev/null
  fi

  sed -i -e "s/^\(CVMFS_CREATOR_VERSION\)=.*/\1=$destination_version/" $server_conf

  # update repository information
  load_repo_config $name
}

_migrate_2_2() {
  local name=$1
  local destination_version="2.3.0-1"
  local server_conf="/etc/cvmfs/repositories.d/${name}/server.conf"

  # get repository information
  load_repo_config $name
  [ ! -z $CVMFS_SPOOL_DIR     ] || die "\$CVMFS_SPOOL_DIR is not set"
  [ ! -z $CVMFS_USER          ] || die "\$CVMFS_USER is not set"
  [ ! -z $CVMFS_UNION_FS_TYPE ] || die "\$CVMFS_UNION_FS_TYPE is not set"

  echo "Migrating repository '$name' from CernVM-FS $(mangle_version_string $CVMFS_CREATOR_VERSION) to $(mangle_version_string $destination_version)"

  echo "--> umount repository"
  run_suid_helper rw_umount $name

  echo "--> updating scratch directory layout"
  local scratch_dir="${CVMFS_SPOOL_DIR}/scratch"
  rm -fR   ${scratch_dir}
  mkdir -p ${scratch_dir}/current
  mkdir -p ${scratch_dir}/wastebin
  chown -R $CVMFS_USER ${scratch_dir}

  echo "--> updating /etc/fstab"
  local comment="added by CernVM-FS for ${name}"
  sed -i -e "s~^\(.*\)\(${scratch_dir}\)\(.*${comment}\)\s*$~\1\2/current\3~" /etc/fstab

  echo "--> remount repository"
  run_suid_helper rw_mount $name

  echo "--> updating server.conf"
  sed -i -e "s/^\(CVMFS_CREATOR_VERSION\)=.*/\1=$destination_version/" $server_conf

  echo "--> ensure binary permission settings"
  ensure_swissknife_suid $CVMFS_UNION_FS_TYPE

  # update repository information
  load_repo_config $name
}

_migrate_2_3_0() {
  local name=$1
  local destination_version="2.3.3-1"
  local server_conf="/etc/cvmfs/repositories.d/${name}/server.conf"

  load_repo_config $name
  echo "Migrating repository '$name' from CernVM-FS $(mangle_version_string $CVMFS_CREATOR_VERSION) to $(mangle_version_string $destination_version)"

  if ! has_global_info_path; then
    echo "--> create info resource (please update server info with 'cvmfs_server update-info')"
    create_global_info_skeleton || die "fail"
  fi

  if ! has_apache_config_for_global_info; then
    echo "--> create Apache configuration for info resource"
    create_apache_config_for_global_info || die "fail (create apache config)"
    reload_apache > /dev/null            || die "fail (reload apache)"
  fi

  echo "--> update global JSON information"
  update_global_repository_info || die "fail"

  echo "--> updating server.conf"
  sed -i -e "s/^\(CVMFS_CREATOR_VERSION\)=.*/\1=$destination_version/" $server_conf

  # update repository information
  load_repo_config $name
}

_migrate_138() {
  local name=$1
  local destination_version="138"
  local server_conf="/etc/cvmfs/repositories.d/${name}/server.conf"
  local apache_conf_path="$(get_apache_conf_path)"
  local apache_conf_file="$(get_apache_conf_filename $name)"
  local apache_repo_conf="$apache_conf_path/$apache_conf_file"
  local apache_info_conf="$apache_conf_path/$(get_apache_conf_filename "info")"
  local do_apache_reload=0

  load_repo_config $name
  echo "Migrating repository '$name' from layout revision $(mangle_version_string $CVMFS_CREATOR_VERSION) to revision $(mangle_version_string $destination_version)"

  if has_apache_config_file "$apache_conf_file"; then
    if _is_generated_apache_conf "$apache_repo_conf"; then
      echo "--> updating apache config ($(basename $apache_repo_conf))"
      local storage_dir=$(get_upstream_config $CVMFS_UPSTREAM_STORAGE)
      local wsgi=""
      is_stratum1 $name && wsgi="enabled"
      create_apache_config_for_endpoint $name $storage_dir $wsgi
      do_apache_reload=1
    else
      echo "--> skipping foreign apache config ($(basename $apache_repo_conf))"
    fi
  fi

  if has_apache_config_for_global_info; then
    if _is_generated_apache_conf "$apache_info_conf"; then
      echo "--> updating apache info config"
      local storage_dir="${DEFAULT_LOCAL_STORAGE}/info"
      create_apache_config_for_endpoint "info" "$storage_dir"
      do_apache_reload=1
    else
      echo "--> skipping foreign apache info config"
    fi
  fi

  if [ $do_apache_reload -eq 1 ]; then
    echo "--> reloading Apache"
    reload_apache > /dev/null
  fi

  local warn_threshold="`sed -n -e 's/^CVMFS_CATALOG_ENTRY_WARN_THRESHOLD=//p' $server_conf`"
  if [ -n "$warn_threshold" ]; then
    echo "--> updating server.conf"
    sed -i -e '/^CVMFS_CATALOG_ENTRY_WARN_THRESHOLD=/d' $server_conf
    local kcatalog_limit="$(($warn_threshold / 1000))"
    # If the default was not changed, the new root catalog default of 200k
    # entries is applied.
    if [ "$kcatalog_limit" != 500 ]; then
      echo "CVMFS_ROOT_KCATALOG_LIMIT=${kcatalog_limit}" >> $server_conf
      echo "CVMFS_NESTED_KCATALOG_LIMIT=${kcatalog_limit}" >> $server_conf
    fi
  fi

  echo "--> updating server.conf"
  sed -i -e "s/^\(CVMFS_CREATOR_VERSION\)=.*/\1=$destination_version/" $server_conf

  # update repository information
  load_repo_config $name
}

_migrate_139() {
  local name=$1
  local destination_version="139"
  local server_conf="/etc/cvmfs/repositories.d/${name}/server.conf"

  load_repo_config $name
  echo "Migrating repository '$name' from layout revision $(mangle_version_string $CVMFS_CREATOR_VERSION) to revision $(mangle_version_string $destination_version)"

  if is_stratum0 $name; then
    echo "--> adjusting /etc/fstab"
    sed -i -e "s|\(.*\),noauto\(.*# added by CernVM-FS for ${CVMFS_REPOSITORY_NAME}\)|\1,noauto,nodev\2|" /etc/fstab

    # Make sure the systemd mount unit exists
    if is_systemd; then
      /usr/lib/systemd/system-generators/systemd-fstab-generator \
        /run/systemd/generator '' '' 2>/dev/null || true
      systemctl daemon-reload
    fi
  fi

  echo "--> updating server.conf"
  sed -i -e "s/^\(CVMFS_CREATOR_VERSION\)=.*/\1=$destination_version/" $server_conf

  # update repository information
  load_repo_config $name
}

_migrate_140() {
  local name=$1
  local destination_version="140"
  local server_conf="/etc/cvmfs/repositories.d/${name}/server.conf"
  local apache_repo_conf="$(get_apache_conf_path)/$(get_apache_conf_filename $name)"

  load_repo_config $name
  echo "Migrating repository '$name' from layout revision $(mangle_version_string $CVMFS_CREATOR_VERSION) to revision $(mangle_version_string $destination_version)"

  # only called when has apache config
  if _is_generated_apache_conf "$apache_repo_conf"; then
    echo "--> updating apache config ($(basename $apache_repo_conf))"
    local storage_dir=$(get_upstream_config $CVMFS_UPSTREAM_STORAGE)
    # only called on stratum1
    local wsgi="enabled"
    create_apache_config_for_endpoint $name $storage_dir $wsgi
    echo "--> reloading Apache"
    reload_apache > /dev/null
  else
    echo "--> skipping foreign apache config ($(basename $apache_repo_conf))"
  fi

  echo "--> updating server.conf"
  sed -i -e "s/^\(CVMFS_CREATOR_VERSION\)=.*/\1=$destination_version/" $server_conf

  # update repository information
  load_repo_config $name
}


_migrate_141() {
  local name=$1
  local destination_version="141"
  local server_conf="/etc/cvmfs/repositories.d/${name}/server.conf"
  local client_conf="/etc/cvmfs/repositories.d/${name}/client.conf"

  load_repo_config $name
  echo "Migrating repository '$name' from layout revision $(mangle_version_string $CVMFS_CREATOR_VERSION) to revision $(mangle_version_string $destination_version)"

  # only called when this is a stratum 0
  if [ -f $client_conf ]; then
    echo "--> updating client.conf"
    if ! grep -q "CVMFS_NFILES" $client_conf; then
      echo "CVMFS_NFILES=65536" >> $client_conf
    else
      sed -i -e "s/^\(CVMFS_NFILES\)=.*/\1=65536/" $client_conf
    fi
  else
    echo "--> skipping client configuration on stratum 1"
  fi

  echo "--> updating server.conf"
  sed -i -e "s/^\(CVMFS_CREATOR_VERSION\)=.*/\1=$destination_version/" $server_conf

  # update repository information
  load_repo_config $name
}


_migrate_142() {
  local name=$1
  local destination_version="142"
  local server_conf="/etc/cvmfs/repositories.d/${name}/server.conf"
  local client_conf="/etc/cvmfs/repositories.d/${name}/client.conf"

  load_repo_config $name
  echo "Migrating repository '$name' from layout revision $(mangle_version_string $CVMFS_CREATOR_VERSION) to revision $(mangle_version_string $destination_version)"

  # only called when this is a stratum 0
  if [ -f $client_conf ]; then
    echo "--> updating client.conf"
    if ! grep -q "CVMFS_TALK_SOCKET" $client_conf; then
      echo "CVMFS_TALK_SOCKET=/var/spool/cvmfs/${name}/cvmfs_io" >> $client_conf
    fi
    if ! grep -q "CVMFS_TALK_OWNER" $client_conf; then
      local cvmfs_user="$(grep ^CVMFS_USER= $server_conf | cut -d= -f2)"
      echo "CVMFS_TALK_OWNER=$cvmfs_user" >> $client_conf
    fi
  else
    echo "--> skipping client configuration on stratum 1"
  fi

  echo "--> updating server.conf"
  if ! grep -q "^CVMFS_IGNORE_XDIR_HARDLINKS=" $server_conf; then
      echo "CVMFS_IGNORE_XDIR_HARDLINKS=true" >> $server_conf
  fi
  sed -i -e "s/^\(CVMFS_CREATOR_VERSION\)=.*/\1=$destination_version/" $server_conf

  # update repository information
  load_repo_config $name
}


_migrate_143() {
  local name=$1
  local destination_version="143"
  local client_conf="/etc/cvmfs/repositories.d/${name}/client.conf"
  local server_conf="/etc/cvmfs/repositories.d/${name}/server.conf"

  load_repo_config $name
  echo "Migrating repository '$name' from layout revision $(mangle_version_string $CVMFS_CREATOR_VERSION) to revision $(mangle_version_string $destination_version)"

  echo "--> updating client.conf"
  if ! grep -q "CVMFS_USE_SSL_SYSTEM_CA" $client_conf; then
    echo "CVMFS_USE_SSL_SYSTEM_CA=true" >> $client_conf
  fi

  if is_stratum0 $name; then
    echo "--> adjusting /etc/fstab"
    sed -i -e "s|\(.*\)allow_other,\(.*# added by CernVM-FS for ${CVMFS_REPOSITORY_NAME}\)|\1allow_other,fsname=${CVMFS_REPOSITORY_NAME},\2|" /etc/fstab

    # Make sure the systemd mount unit exists
    if is_systemd; then
      /usr/lib/systemd/system-generators/systemd-fstab-generator \
        /run/systemd/generator '' '' 2>/dev/null || true
      systemctl daemon-reload
    fi
  fi

  echo "--> updating server.conf"
  sed -i -e "s/^\(CVMFS_CREATOR_VERSION\)=.*/\1=$destination_version/" $server_conf

  # update repository information
  load_repo_config $name
}


cvmfs_server_migrate() {
  local names
  local retcode=0

  # get repository names
  check_parameter_count_for_multiple_repositories $#
  names=$(get_or_guess_multiple_repository_names "$@")
  check_multiple_repository_existence "$names"

  # sanity checks
  is_root || die "Only root can migrate repositories"

  for name in $names; do

    check_repository_existence $name || { echo "The repository $name does not exist"; retcode=1; continue; }

    # get repository information
    load_repo_config $name
    creator="$(repository_creator_version $name)"

    # more sanity checks
    is_owner_or_root $name || { echo "Permission denied: Repository $name is owned by $user"; retcode=1; continue; }
    check_repository_compatibility $name "nokill" && { echo "Repository '$name' is already up-to-date."; continue; }
    health_check -r $name

    if is_stratum0 $name && is_in_transaction $name; then
      echo "Repository '$name' is currently in a transaction - migrating might"
      echo "result in data loss. Please abort or publish this transaction with"
      echo "the CernVM-FS version ($creator) that opened it."
      retcode=1
      continue
    fi

    # do the migrations...
    if [ x"$creator" = x"2.1.6" ]; then
      _migrate_2_1_6 $name
      creator="$(repository_creator_version $name)"
    fi

    if [ x"$creator" = x"2.1.7" -o  \
         x"$creator" = x"2.1.8" -o  \
         x"$creator" = x"2.1.9" -o  \
         x"$creator" = x"2.1.10" -o \
         x"$creator" = x"2.1.11" -o \
         x"$creator" = x"2.1.12" -o \
         x"$creator" = x"2.1.13" -o \
         x"$creator" = x"2.1.14" ];
    then
      _migrate_2_1_7 $name
      creator="$(repository_creator_version $name)"
    fi

    if [ x"$creator" = x"2.1.15" -o   \
         x"$creator" = x"2.1.16" -o   \
         x"$creator" = x"2.1.17" -o   \
         x"$creator" = x"2.1.18" -o   \
         x"$creator" = x"2.1.19" ] && \
         is_stratum1 $name         && \
         is_local_upstream $CVMFS_UPSTREAM_STORAGE;
    then
      _migrate_2_1_15 $name
      creator="$(repository_creator_version $name)"
    fi

    if [ x"$creator" = x"2.1.15" -o \
         x"$creator" = x"2.1.16" -o \
         x"$creator" = x"2.1.17" -o \
         x"$creator" = x"2.1.18" -o \
         x"$creator" = x"2.1.19" -o \
         x"$creator" = x"2.1.20" -o \
         x"$creator" = x"2.2.0-0" ];
    then
      _migrate_2_1_20 $name
      creator="$(repository_creator_version $name)"
    fi

    if [ x"$creator" = x"2.2.0-1" -o   \
         x"$creator" = x"2.2.1-1" -o   \
         x"$creator" = x"2.2.2-1" -o   \
         x"$creator" = x"2.2.3-1" ] && \
         is_stratum0 $name;
    then
      _migrate_2_2 $name
      creator="$(repository_creator_version $name)"
    fi

    if [ x"$creator" = x"2.2.0-1" -o   \
         x"$creator" = x"2.2.1-1" -o   \
         x"$creator" = x"2.2.2-1" -o   \
         x"$creator" = x"2.2.3-1" -o   \
         x"$creator" = x"2.3.0-1" -o   \
         x"$creator" = x"2.3.1-1" -o   \
         x"$creator" = x"2.3.2-1" ]; then
      _migrate_2_3_0 $name
      creator="$(repository_creator_version $name)"
    fi

    if [ x"$creator" = x"2.3.3-1" -o   \
         x"$creator" = x"2.3.4-1" -o   \
         x"$creator" = x"2.3.5-1" -o   \
         x"$creator" = x"2.3.6-1" -o   \
         x"$creator" = x"2.4.0-1" ]; then
      # initially this was version 137 but it does everything needed by
      #  138 so skip up to 138.
      _migrate_138 $name
      creator="$(repository_creator_version $name)"
    fi

    if [ "$creator" = "137" ] && \
         is_stratum1 $name && \
         has_apache_config_file $(get_apache_conf_filename $name); then
      # this does slightly more than needed but is close enough so reuse it
      _migrate_138 $name
      creator="$(repository_creator_version $name)"
    fi

    if [ $creator -lt 139 ]; then
      _migrate_139 $name
      creator="$(repository_creator_version $name)"
    fi

    if [ "$creator" -lt 140 ] && \
         is_stratum1 $name && \
         has_apache_config_file $(get_apache_conf_filename $name); then
      _migrate_140 $name
      creator="$(repository_creator_version $name)"
    fi

    if [ "$creator" -lt 141 ] && is_stratum0 $name; then
      _migrate_141 $name
      creator="$(repository_creator_version $name)"
    fi

    if [ "$creator" -lt 142 ] && is_stratum0 $name; then
      _migrate_142 $name
      creator="$(repository_creator_version $name)"
    fi

    if [ "$creator" -lt 143 ] && is_stratum0 $name; then
      _migrate_143 $name
      creator="$(repository_creator_version $name)"
    fi

  done

  syncfs

  return $retcode
}


