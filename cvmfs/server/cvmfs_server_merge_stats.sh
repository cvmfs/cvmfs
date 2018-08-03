#
# This file is part of the CernVM File System
#
# Implementation of the "cvmfs_server stats command"

clean_up() {
    echo "Cleaning up"
    echo "  Removing temporary files"
    rm -rvf /tmp/cvmfs_server_merge_stats/*
}

# merge publish_statistics table
cvmfs_server_merge_table() {
  local db_file_1=$1
  local db_file_2=$2
  local output_db=$3
  local TMP_DIR=/tmp/cvmfs_server_merge_stats

  mkdir -p $TMP_DIR
  sqlite3 $db_file_1 "SELECT key, value from properties" > $TMP_DIR/properties_values_1
  sqlite3 $db_file_2 "SELECT key, value from properties" > $TMP_DIR/properties_values_2

  local repo_name_1=$(cat $TMP_DIR/properties_values_1 | grep repo_name | cut -d '|' -f 2)
  local repo_name_2=$(cat $TMP_DIR/properties_values_2 | grep repo_name | cut -d '|' -f 2)
  local schema_1=$(cat $TMP_DIR/properties_values_1 | grep schema | cut -d '|' -f 2)
  local schema_2=$(cat $TMP_DIR/properties_values_2 | grep schema | cut -d '|' -f 2)
  local schema_revision_1=$(cat $TMP_DIR/properties_values_1 | grep schema_revision | cut -d '|' -f 2)
  local schema_revision_2=$(cat $TMP_DIR/properties_values_2 | grep schema_revision | cut -d '|' -f 2)

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

  echo ".dump publish_statistics" > $TMP_DIR/script_publish_statistics
  echo ".dump properties" > $TMP_DIR/script_properties

  # get properties table
  sqlite3 $db_file_1 < $TMP_DIR/script_properties > $TMP_DIR/properties_table.txt
  # get publish_statistics table from the first database file
  sqlite3 $db_file_1 < $TMP_DIR/script_publish_statistics > $TMP_DIR/publish_statistics_table1.txt
  # get publish_statistics table from the second database file
  sqlite3 $db_file_2 < $TMP_DIR/script_publish_statistics > $TMP_DIR/publish_statistics_table2.txt

  cat $TMP_DIR/properties_table.txt > $TMP_DIR/new_db.txt
  cat $TMP_DIR/publish_statistics_table1.txt | grep BEGIN >> $TMP_DIR/new_db.txt
  cat $TMP_DIR/publish_statistics_table1.txt | grep CREATE >> $TMP_DIR/new_db.txt
  # Add insert statements from the first database file
  cat $TMP_DIR/publish_statistics_table1.txt | grep INSERT >> $TMP_DIR/new_db.txt
  # Add insert statements from the second database file
  cat $TMP_DIR/publish_statistics_table2.txt | grep INSERT >> $TMP_DIR/new_db.txt
  cat $TMP_DIR/publish_statistics_table2.txt | grep COMMIT >> $TMP_DIR/new_db.txt
  echo "" > $output_db  # make sure the output file is empty

  # Merge!
  sqlite3 $output_db < $TMP_DIR/new_db.txt
  echo "Success: $1 and $2 publish_statistics tables were merged in $3 ."
  clean_up $TMP_DIR
  return 0
}


cvmfs_server_merge_stats() {
  trap clean_up EXIT HUP INT TERM || return $?
  local output_db="output.db"   # default output file
  local db_file_1=""
  local db_file_2=""

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

  shift $(($OPTIND-1))

  check_parameter_count 2 $#
  cvmfs_server_merge_table $1 $2 $output_db
  return $?
}

