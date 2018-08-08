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
  local table=$4
  local TMP_DIR=/tmp/cvmfs_server_merge_stats
  local create_table_statement=""
  local columns=""

  mkdir -p $TMP_DIR
  echo ".dump $table" > $TMP_DIR/script_${table}
  echo ".dump ${table}1" > $TMP_DIR/script_${table}1
  echo ".dump ${table}2" > $TMP_DIR/script_${table}2

  sqlite3 $1 < $TMP_DIR/script_${table} > $TMP_DIR/${table}.txt
  create_table_statement="$(cat $TMP_DIR/${table}.txt | grep CREATE)"
  sqlite3 $1 "ALTER table $table RENAME TO ${table}1;"
  sqlite3 $2 "ALTER table $table RENAME TO ${table}2;"

  sqlite3 $1 < $TMP_DIR/script_${table}1 > $TMP_DIR/${table}1.txt
  sqlite3 $2 < $TMP_DIR/script_${table}2 > $TMP_DIR/${table}2.txt

  cat $TMP_DIR/${table}1.txt > $TMP_DIR/new_db.txt
  cat $TMP_DIR/${table}2.txt >> $TMP_DIR/new_db.txt

  # list with all columns separated by  ','
  sqlite3 $1 -header -separator "," "Select * from ${table}1;" | head -1 > $TMP_DIR/all_columns.txt
  # Eliminate first column (*_id) -- PRIMARY KEY
  cut -d ',' -f2- $TMP_DIR/all_columns.txt > $TMP_DIR/columns.txt
  columns="$(cat $TMP_DIR/columns.txt)"

  # Merge!
  sqlite3 $output_db < $TMP_DIR/new_db.txt      # create ${table}1 and ${table}2 (with data)
  sqlite3 $output_db "$create_table_statement"  # create ${table}
  sqlite3 $output_db "insert into ${table} select * from ${table}1;"
  sqlite3 $output_db "insert into ${table} ($columns) select $columns from ${table}2;"
  sqlite3 $output_db "drop table ${table}1"     
  sqlite3 $output_db "drop table ${table}2"

  # Undo changes into input db files
  sqlite3 $1 "ALTER table ${table}1 RENAME TO ${table};"
  sqlite3 $2 "ALTER table ${table}2 RENAME TO ${table};"

  echo "Success: $1 and $2 ${table} tables were merged in $output_db"

  clean_up $TMP_DIR
  return 0
}

cvmfs_server_merge_checks() {
  local db_file_1=$1
  local db_file_2=$2
  local output_db=$3
  local TMP_DIR=/tmp/cvmfs_server_merge_stats

  mkdir -p $TMP_DIR
  sqlite3 $db_file_1 "SELECT * from properties" > $TMP_DIR/properties_values_1
  sqlite3 $db_file_2 "SELECT * from properties" > $TMP_DIR/properties_values_2

  local repo_name_1="$(cat $TMP_DIR/properties_values_1 | grep repo_name | cut -d '|' -f 2)"
  local repo_name_2="$(cat $TMP_DIR/properties_values_2 | grep repo_name | cut -d '|' -f 2)"
  local schema_1="$(cat $TMP_DIR/properties_values_1 | grep schema | cut -d '|' -f 2)"
  local schema_2="$(cat $TMP_DIR/properties_values_2 | grep schema | cut -d '|' -f 2)"
  local schema_revision_1="$(cat $TMP_DIR/properties_values_1 | grep schema_revision | cut -d '|' -f 2)"
  local schema_revision_2="$(cat $TMP_DIR/properties_values_2 | grep schema_revision | cut -d '|' -f 2)"

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

  # Create properties table in the output db file and insert data into it
  echo ".dump properties" > $TMP_DIR/script_properties
  sqlite3 $1 < $TMP_DIR/script_properties > $TMP_DIR/properties_table.txt
  cat $TMP_DIR/properties_table.txt > $TMP_DIR/new_db.txt
  sqlite3 $output_db < $TMP_DIR/new_db.txt
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

  echo "" > $output_db
  cvmfs_server_merge_checks $1 $2 $output_db
  cvmfs_server_merge_table $1 $2 $output_db "publish_statistics"
  cvmfs_server_merge_table $1 $2 $output_db "gc_statistics"
  return $?
}
