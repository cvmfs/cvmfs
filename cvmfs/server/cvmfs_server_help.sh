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

  CVMFS_SERVER_SUBCOMMAND_SHORT="$(eval echo '"${'$(echo "_CVMFS_DOC_${command}_SHORT" |tr '[a-z]-' '[A-Z]_')'}"')"
  CVMFS_SERVER_SUBCOMMAND_SYNOPSIS="$(eval echo '"${'$(echo "_CVMFS_DOC_${command}_SYNOPSIS" |tr '[a-z]-' '[A-Z]_')'}"')"
  CVMFS_SERVER_SUBCOMMAND_OPTIONS="$(eval echo '"${'$(echo "_CVMFS_DOC_${command}_OPTIONS" |tr '[a-z]-' '[A-Z]_')'}"')"
  CVMFS_SERVER_SUBCOMMAND_EXAMPLES="$(eval echo '"${'$(echo "_CVMFS_DOC_${command}_EXAMPLES" |tr '[a-z]-' '[A-Z]_')'}"')"

  echo "\
cvmfs_server $command - $CVMFS_SERVER_SUBCOMMAND_SHORT

Synopsis: cvmfs_server $command $CVMFS_SERVER_SUBCOMMAND_SYNOPSIS" >&2


  if [ x"$CVMFS_SERVER_SUBCOMMAND_OPTIONS" != x"" ]; then
    echo "
Options:" >&2
    echo "$CVMFS_SERVER_SUBCOMMAND_OPTIONS" |
    while read -r option; do
      option_key="$(echo "$option" |cut -d : -f 1)"
      option_value="$(echo "$option" |cut -d : -f 2-)"
      echo "    -$option_key : $option_value" >&2
    done
  fi


  if [ x"$CVMFS_SERVER_SUBCOMMAND_EXAMPLES" != x"" ]; then
    echo "
Examples:
$(echo "$CVMFS_SERVER_SUBCOMMAND_EXAMPLES" |sed 's/^/    /')" >&2
  fi
}
