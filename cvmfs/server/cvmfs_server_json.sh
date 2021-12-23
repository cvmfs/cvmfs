#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# JSON "API" related functions

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh


get_global_info_path() {
  echo "${DEFAULT_LOCAL_STORAGE}/info"
}


get_global_info_v1_path() {
  echo "$(get_global_info_path)/v${LATEST_JSON_INFO_SCHEMA}"
}


_write_info_file() {
  local info_file="${1}.json"
  local info_file_dir="$(get_global_info_v1_path)"
  local info_file_path="${info_file_dir}/${info_file}"
  local tmp_file="${info_file_dir}/${info_file}.txn.$(date +%s)"

  cat - > $tmp_file
  chmod 0644 $tmp_file
  mv -f $tmp_file $info_file_path
  set_selinux_httpd_context_if_needed $info_file_dir
}


_check_info_file() {
  local info_file="${1}.json"
  cvmfs_sys_file_is_regular "$(get_global_info_v1_path)/${info_file}"
}


_available_repos() {
  local filter="$1"
  local repo=""
  local repo_cfg_path="/etc/cvmfs/repositories.d"

  [ $(ls $repo_cfg_path | wc -l) -gt 0 ] || return 0
  for repository in ${repo_cfg_path}/*; do
    repo=$(basename $repository)
    if ( [ x"$filter" = x"" ]                              ) || \
       ( [ x"$filter" = x"stratum0" ] && is_stratum0 $repo ) || \
       ( [ x"$filter" = x"stratum1" ] && is_stratum1 $repo ); then
      echo $repo
    fi
  done
}


_render_repos() {
  local i=$#

  for repo in $@; do
    CVMFS_PASSTHROUGH=false
    load_repo_config $repo

    echo '    {'
    echo '      "name"  : "'$CVMFS_REPOSITORY_NAME'",'
    if [ x"$CVMFS_REPOSITORY_NAME" != x"$repo" ]; then
      echo '      "alias" : "'$repo'",'
    fi
    if [ x"$CVMFS_PASSTHROUGH" = x"true" ]; then
      echo '      "pass-through" : true,'
    fi
    echo '      "url"   : "/cvmfs/'$repo'"'
    echo -n '    }'

    i=$(( $i - 1 ))
    [ $i -gt 0 ] && echo "," || echo ""
  done
}


_render_info_file() {
  echo '{'
  echo '  "schema"       : '$LATEST_JSON_INFO_SCHEMA','
  echo '  "repositories" : ['

  _render_repos $(_available_repos "stratum0")

  echo '  ],'
  echo '  "replicas" : ['

  _render_repos $(_available_repos "stratum1")

  echo '  ]'
  echo '}'
}


has_global_info_path() {
  [ -d $(get_global_info_path) ] && [ -d $(get_global_info_v1_path) ]
}


update_global_repository_info() {
  # sanity checks
  has_global_info_path || return 1
  is_root              || return 2

  _render_info_file | _write_info_file "repositories"
}


update_global_meta_info() {
  local meta_info_file="$1"
  has_global_info_path || return 1
  is_root              || return 2

  cat "$meta_info_file" | _write_info_file "meta"
}


get_editor() {
  local editor=${EDITOR:=vi}
  if ! which $editor  > /dev/null 2>&1; then
    echo  "Didn't find editor '$editor'." 1>&2
    echo "Consider to use the \$EDITOR environment variable" 1>&2
    exit 1
  fi
  echo $editor
}


has_jq() {
  which jq > /dev/null 2>&1
}

check_jq() {
  local hasjq=1
  if ! has_jq; then
    hasjq=0
    echo 1>&2
    echo "Warning: Didn't find 'jq' on your system. It is your responsibility" 1>&2
    echo "         to produce a valid JSON file." 1>&2
    echo 1>&2
    read -p "  Press any key to continue..." nirvana
  fi
  echo $hasjq
}


validate_json() {
  local json_file="$1"

  if ! which jq > /dev/null 2>&1; then
    return 0 # no jq -> assume JSON is valid
  fi

  jq '.' $json_file 2>&1
}


edit_json_until_valid() {
  local json_file="$1"
  local editor=$(get_editor)
  local has_jq=$(check_jq)

  local retval=0
  while true; do
    $editor $json_file < $(tty) > $(tty) 2>&1
    [ $has_jq -eq 1 ] || break

    local jq_output=""
    local retry=""
    if ! jq_output=$(validate_json $json_file); then
      echo
      echo "Your JSON file is invalid, please check again:"
      echo "$jq_output"
      read -p "Edit again? [y]: " retry
      if [ x"$retry" != x"y" ] && \
         [ x"$retry" != x"Y" ] && \
         [ x"$retry" != x""  ]; then
        retval=1
        break
      fi
    else
      break
    fi
  done

  return $retval
}

# This updates a variable in the repository status file .cvmfs_status.json
# It assumes that it is running under a repository lock
# @param name      the name of the repository to update
# @param variable  the status variable to update
# @param value     the value to set the variable to
update_repo_status() {
  if ! has_jq; then
    # silently do nothing if there is no jq
    return
  fi

  local name="$1"
  local variable="$2"
  local value="$3"

  local old_status="$(read_repo_item $name .cvmfs_status.json)"
  if [ -z "$old_status" ]; then
    old_status="{}"
  fi

  load_repo_config $name
  local user_shell="$(get_user_shell $name)"

  local jq_tmp="${CVMFS_SPOOL_DIR}/tmp/status.json"
  echo "$old_status" | jq ".$variable=\"$value\"" | $user_shell "cat > $jq_tmp"
  $user_shell "$(__swissknife_cmd) upload -r ${CVMFS_UPSTREAM_STORAGE} \
    -i $jq_tmp                                                     \
    -o .cvmfs_status.json"
  $user_shell "rm -f $jq_tmp"
}


get_json_field() {
  has_jq || return ""

  local snippet="$1"
  local field="$2"
  echo "$snippet" | jq -r ".$field // empty"
}
