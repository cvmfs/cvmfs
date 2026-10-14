#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server validate" command

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh


# Ensure a string is properly escaped for JSON output.
_validate_json_string() {
  printf '%s' "$1" | awk '
    NR > 1 { out = out "\\n" }
    {
      for (i = 1; i <= length($0); i++) {
        c = substr($0, i, 1)
        if (c == "\\") out = out "\\\\"
        else if (c == "\"") out = out "\\\""
        else if (c == "\b") out = out "\\b"
        else if (c == "\f") out = out "\\f"
        else if (c == "\r") out = out "\\r"
        else if (c == "\t") out = out "\\t"
        else out = out c
      }
    }
    END { printf "\"%s\"", out }
  '
}


__do_validate() {
  local name

  check_parameter_count_with_guessing $#
  name=$(get_or_guess_repository_name $1)

  check_repository_existence $name || die "The repository $name does not exist"
  load_repo_config $name
  is_owner_or_root $name || die "Permission denied: Repository $name is owned by $CVMFS_USER"

  local validator_bin="/usr/libexec/cvmfs/cvmfs_validator"
  if [ ! -x "$validator_bin" ]; then
    echo "Error: no validation engine available"
    echo "Build CernVM-FS with -DBUILD_CONFIG_VALIDATOR=ON"
    return 1
  fi

  local server_conf="/etc/cvmfs/repositories.d/${name}/server.conf"
  [ -r "$server_conf" ] || die "Cannot read $server_conf"

  # Build a JSON object from server.conf, read strictly from the file (not
  # the ambient shell) so a stray CVMFS_*/X509_* variable in the caller's
  # environment can't leak into the config being validated. Empty values
  # count as unset and are dropped, matching the config's own convention.
  local varname value sep=""
  local json="{"
  for varname in $(grep -o '^[A-Za-z0-9_]*=' "$server_conf" | tr -d '=' \
                      | sort -u | grep '^CVMFS_\|^X509_'); do
    unset "$varname"
    eval "$(grep "^${varname}=" "$server_conf" | tail -n 1)"
    eval "value=\$$varname"
    [ -z "$value" ] && continue
    # Every value is emitted as a JSON string; the validator converts the
    # ones the schema declares as integers or booleans.
    json="$json$sep
  $(_validate_json_string "$varname"): $(_validate_json_string "$value")"
    sep=","
  done
  json="$json
}"

  if echo "$json" | "$validator_bin" -d '#ServerConfig'; then
    echo "Configuration of $name is valid"
    return 0
  else
    echo "Configuration of $name is invalid"
    return 1
  fi
}


cvmfs_server_validate() {
  local do_all=0

  # optional parameter handling
  OPTIND=1
  while getopts "a" option
  do
    case $option in
      a)
        do_all=1
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command validate: Unrecognized option: $1"
      ;;
    esac
  done
  shift $(($OPTIND-1))

  if [ $do_all -eq 1 ]; then
    [ $# -eq 0 ] || die "no non-option parameters expected with -a"

    local retval=0
    local repository
    for repository in /etc/cvmfs/repositories.d/*; do
      [ "x$repository" = "x/etc/cvmfs/repositories.d/*" ] && break
      __do_validate $(basename $repository) || retval=1
    done
    return $retval
  fi

  __do_validate "$@"
}
