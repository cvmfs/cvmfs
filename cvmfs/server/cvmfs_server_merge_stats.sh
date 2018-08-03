#
# This file is part of the CernVM File System
#
# Implementation of the "cvmfs_server stats command"


cvmfs_server_merge_stats() {
  sqlite3 $1 "SELECT key, value from properties" > properties_values_1
  sqlite3 $2 "SELECT key, value from properties" > properties_values_2

  local repo_name_1=$(cat properties_values_1 | grep repo_name | cut -d '|' -f 2)
  local repo_name_2=$(cat properties_values_2 | grep repo_name | cut -d '|' -f 2)
  local schema_1=$(cat properties_values_1 | grep schema | cut -d '|' -f 2)
  local schema_2=$(cat properties_values_2 | grep schema | cut -d '|' -f 2)
  local schema_revision_1=$(cat properties_values_1 | grep schema_revision | cut -d '|' -f 2)
  local schema_revision_2=$(cat properties_values_2 | grep schema_revision | cut -d '|' -f 2)

  # Sanity checks
  if [ "x$repo_name_1" != "x$repo_name_2" ]; then
    echo "The given db files have different repo_name: $repo_name_1 vs $repo_name_2!"
    return 1
  fi
  if [ "x$schema_1" != "x$schema_2" ]; then
    echo "The given db files have different schema: $schema_1 vs $schema_2!"
    return 1
  fi
  if [ "x$schema_revision_1" != "x$schema_revision_2" ]; then
    echo "The given db files have different schema_revision: $schema_revision_1 vs $schema_revision_2!"
    return 1
  fi


  echo ".dump publish_statistics" > script_publish_statistics
  echo ".dump properties" > script_properties

  # get properties table
  sqlite3 $1 < script_properties > properties_table.txt
  # get publish_statistics table from the first database file
  sqlite3 $1 < script_publish_statistics > publish_statistics_table1.txt
  # get publish_statistics table from the second database file
  sqlite3 $2 < script_publish_statistics > publish_statistics_table2.txt

  cat properties_table.txt > tmp.txt
  cat publish_statistics_table1.txt | grep BEGIN >> tmp.txt
  cat publish_statistics_table1.txt | grep CREATE >> tmp.txt
  # Add insert statements from the first database file
  cat publish_statistics_table1.txt | grep INSERT >> tmp.txt
  # Add insert statements from the second database file
  cat publish_statistics_table2.txt | grep INSERT >> tmp.txt
  cat publish_statistics_table2.txt | grep COMMIT >> tmp.txt
  echo "" > $3  # make sure the output file is empty

  # Merge!
  sqlite3 $3 < tmp.txt
  echo "Files $1 and $2 were merged in $3"
  # clean
  rm tmp.txt
  rm script_publish_statistics
  rm script_properties
  rm properties_values_*

  return 0
}


cvmfs_server_merge_stats() {
  local output_db="output.db"   # default output file
  local db_file_1=""
  local db_file_2=""
  local merge_option=0

  # optional parameter handling
  OPTIND=1
  while getopts "o:" option
  do
    case $option in
      o)
        output_db="$OPTARG"
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command merge-stats: Unrecognized option: $1"
      ;;
    esac
  done

  echo $@
  # cvmfs_server_merge_stats $db_file_1 $db_file_2 $output_db
}

