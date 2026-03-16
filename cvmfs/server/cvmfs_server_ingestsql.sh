#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server ingestsql" command
# Convenience wrapper around cvmfs_swissknife ingestsql

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh


cvmfs_server_ingestsql() {
  local sqlite_db=""
  local name=""
  local lease_path=""
  local prefix=""
  local num_jobs=""
  local config_prefix=""
  local block_visibility=""
  local reset_ttl=""
  local lease_retry=""
  local priority=""
  local create_db=""
  local allow_additions=0
  local allow_deletions=0
  local force_cancel_lease=0
  local enable_core=0
  local create_catalogs=0
  local check_completed=0
  local verbose=0

  # passthrough options to collect
  local extra_args=""

  while [ "$2" != "" ]; do
    case $1 in
      -D | --database )
        sqlite_db=$2
        ;;
      -l | --lease )
        lease_path=$2
        ;;
      -p | --prefix )
        prefix=$2
        ;;
      -q | --jobs )
        num_jobs=$2
        ;;
      -C | --config-prefix )
        config_prefix=$2
        ;;
      -B | --block-visibility )
        block_visibility=$2
        ;;
      -T | --reset-ttl )
        reset_ttl=$2
        ;;
      -r | --lease-retry )
        lease_retry=$2
        ;;
      -P | --priority )
        priority=$2
        ;;
      -n | --create-db )
        create_db=$2
        ;;
      -a | --allow-additions )
        allow_additions=1
        shift
        continue
        ;;
      -d | --allow-deletions )
        allow_deletions=1
        shift
        continue
        ;;
      -x | --force-cancel-lease )
        force_cancel_lease=1
        shift
        continue
        ;;
      -c | --enable-core )
        enable_core=1
        shift
        continue
        ;;
      -z | --create-catalogs )
        create_catalogs=1
        shift
        continue
        ;;
      -Z | --check-completed )
        check_completed=1
        shift
        continue
        ;;
      -v | --verbose )
        verbose=1
        shift
        continue
        ;;
    esac
    shift
  done

  name=$1

  if [ x"$name" = "x" ]; then
    die "Please provide a repository name as the last argument"
  fi

  if [ x"$sqlite_db" = "x" ] && [ x"$create_db" = "x" ]; then
    die "Please provide a SQLite database with -D <path>"
  fi

  load_repo_config $name

  # sanity checks
  is_stratum0 $name   || die "This is not a stratum 0 repository"
  is_owner_or_root $name || die "Permission denied: Repository $name is owned by $CVMFS_USER"
  check_repository_compatibility $name

  local spool_dir=$CVMFS_SPOOL_DIR
  local stratum0=$CVMFS_STRATUM0
  local upstream=$CVMFS_UPSTREAM_STORAGE
  local upstream_type=$(get_upstream_type $upstream)
  local gw_key_file=/etc/cvmfs/keys/${name}.gw

  # Build the swissknife ingestsql command
  local ingestsql_command="$(__swissknife_cmd dbg) \
    ingestsql                                      \
    -N $name                                       \
    -w $stratum0                                   \
    -t ${spool_dir}/tmp                            \
    -k $CVMFS_PUBLIC_KEY                           \
    -s $gw_key_file                                \
    $(get_swissknife_proxy)                        \
    "

  if [ ! x"$sqlite_db" = "x" ]; then
    ingestsql_command="$ingestsql_command -D $sqlite_db"
  fi

  if [ ! x"$lease_path" = "x" ]; then
    ingestsql_command="$ingestsql_command -l $lease_path"
  fi

  if [ ! x"$prefix" = "x" ]; then
    ingestsql_command="$ingestsql_command -p $prefix"
  fi

  if [ ! x"$num_jobs" = "x" ]; then
    ingestsql_command="$ingestsql_command -q $num_jobs"
  fi

  if [ ! x"$config_prefix" = "x" ]; then
    ingestsql_command="$ingestsql_command -C $config_prefix"
  fi

  if [ ! x"$block_visibility" = "x" ]; then
    ingestsql_command="$ingestsql_command -B $block_visibility"
  fi

  if [ ! x"$reset_ttl" = "x" ]; then
    ingestsql_command="$ingestsql_command -T $reset_ttl"
  fi

  if [ ! x"$lease_retry" = "x" ]; then
    ingestsql_command="$ingestsql_command -r $lease_retry"
  fi

  if [ ! x"$priority" = "x" ]; then
    ingestsql_command="$ingestsql_command -P $priority"
  fi

  if [ ! x"$create_db" = "x" ]; then
    ingestsql_command="$ingestsql_command -n $create_db"
  fi

  if [ $allow_additions -eq 1 ]; then
    ingestsql_command="$ingestsql_command -a"
  fi

  if [ $allow_deletions -eq 1 ]; then
    ingestsql_command="$ingestsql_command -d"
  fi

  if [ $force_cancel_lease -eq 1 ]; then
    ingestsql_command="$ingestsql_command -x"
  fi

  if [ $enable_core -eq 1 ]; then
    ingestsql_command="$ingestsql_command -c"
  fi

  if [ $create_catalogs -eq 1 ]; then
    ingestsql_command="$ingestsql_command -z"
  fi

  if [ $check_completed -eq 1 ]; then
    ingestsql_command="$ingestsql_command -Z"
  fi

  if [ $verbose -eq 1 ]; then
    ingestsql_command="$ingestsql_command -v"
  fi

  # S3 config: extract from upstream storage if it's S3
  if is_s3_upstream $upstream; then
    local s3_config=$(get_upstream_config $upstream | sed 's/.*@//')
    ingestsql_command="$ingestsql_command -3 $s3_config"
  fi

  # Gateway URL: extract from upstream if it's a gateway
  if [ x"$upstream_type" = xgw ]; then
    local gw_url=$(echo $upstream | cut -d, -f3)
    ingestsql_command="$ingestsql_command -g $gw_url"
  fi

  local user_shell="$(get_user_shell $name)"
  $user_shell "$ingestsql_command" || die "ingestsql failed\n\nExecuted Command:\n$ingestsql_command"
}
