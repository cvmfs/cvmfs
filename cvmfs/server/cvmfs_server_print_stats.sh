#
# This file is part of the CernVM File System
#
# Implementation of the "cvmfs_server stats command"

cvmfs_server_print_stats() {
  local output_file=""
  local repo_name=""
  local repo_stats=""
  local separator='|'
  local db_table="publish_statistics"

  # optional parameter handling
  OPTIND=1
  while getopts "o:s:t:" option
  do
    case $option in
      o)
        output_file="$OPTARG"
      ;;
      s)
        separator="$OPTARG"
      ;;
      t)
        db_table="$OPTARG"
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command print-stats: Unrecognized option: $1"
      ;;
    esac
  done

  shift $(($OPTIND-1))
  check_parameter_count 1 $#
  repo_name=$(get_repository_name $1)
  check_multiple_repository_existence $repo_name
  repo_stats="/var/spool/cvmfs/$repo_name/stats.db"
  if [ -e $repo_stats ]; then
    # On an older Linux > /dev/stdout does not work
    if [ "x$output_file" = "x" ]; then
      sqlite3 -header $repo_stats "SELECT * from $db_table;" | tr \| $separator
    else
      sqlite3 -header $repo_stats "SELECT * from $db_table;" | tr \| $separator > $output_file
    fi
  else
    echo "No statistics database file for $repo_name repository."
  fi
}
