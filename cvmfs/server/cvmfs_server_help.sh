#
# This file is part of the CernVM File System
#
# Implementation of the "cvmfs_server help" command

# This file depends on fuctions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh

cvmfs_server_help() {
  check_parameter_count 1 $#
  local CVMFS_SERVER_SUBCOMMAND_SHORT=""
  local CVMFS_SERVER_SUBCOMMAND_SYNOPSIS=""
  local CVMFS_SERVER_SUBCOMMAND_OPTIONS=""
  local CVMFS_SERVER_SUBCOMMAND_EXAMPLES=""
  local command="$1"

  if ! is_subcommand "$command"; then
    usage "No manual entry for $command"
  fi

  CVMFS_SERVER_SUBCOMMAND_SHORT="$(echo "_CVMFS_SERVER_${command^^}_SHORT" |sed 's/-/_/')"
  CVMFS_SERVER_SUBCOMMAND_SYNOPSIS="$(echo "_CVMFS_SERVER_${command^^}_SYNOPSIS" |sed 's/-/_/')"
  CVMFS_SERVER_SUBCOMMAND_OPTIONS="$(echo "_CVMFS_SERVER_${command^^}_OPTIONS" |sed 's/-/_/')"
  CVMFS_SERVER_SUBCOMMAND_EXAMPLES="$(echo "_CVMFS_SERVER_${command^^}_EXAMPLES" |sed 's/-/_/')"

  echo "\
cvmfs_server $command - ${!CVMFS_SERVER_SUBCOMMAND_SHORT}

Synopsis: ${!CVMFS_SERVER_SUBCOMMAND_SYNOPSIS}

Options:" >&2
  options_array="${CVMFS_SERVER_SUBCOMMAND_OPTIONS}[@]"
  for option in "${!options_array}"; do
    option_key="$(echo "$option" |cut -d : -f 1)"
    option_value="$(echo "$option" |cut -d : -f 2-)"
    echo "    -$option_key : $option_value" >&2
  done

  if [ x"${!CVMFS_SERVER_SUBCOMMAND_EXAMPLES}" != x"" ]; then
    echo "
Examples:
${!CVMFS_SERVER_SUBCOMMAND_EXAMPLES}" >&2
  fi
}
