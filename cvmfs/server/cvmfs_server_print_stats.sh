#
# This file is part of the CernVM File System
#
# Implementation of the "cvmfs_server stats command"

_CVMFS_SERVER_PRINT_STATS_SHORT="Print statistics values for a table (default publish_statistics) using the separator specified"
_CVMFS_SERVER_PRINT_STATS_DESCRIPTION="TODO"
_CVMFS_SERVER_PRINT_STATS_SYNOPSIS="_cvmfs_server print-stats_ [options] <fqrn>"
_CVMFS_SERVER_PRINT_STATS_OPTIONS="\
o:output_file
s:char separator (default '|')
t:table_name"

cvmfs_server_print_stats() {
  local output_file="/dev/stdout"
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
    sqlite3 $repo_stats -header -separator $separator "SELECT * from $db_table;" > $output_file
  else
    echo "No statistics database file for $repo_name repository."
  fi
}
