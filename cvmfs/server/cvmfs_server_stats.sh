#
# This file is part of the CernVM File System
#
# Implementation of the "cvmfs_server stats command"


cvmfs_server_merge_stats() {
  sqlite3 $1 "SELECT key, value from properties" > properties_values_1
  sqlite3 $2 "SELECT key, value from properties" > properties_values_2

  local repo_name_1=$(cat properties_values_1 | grep repo_name | cut -d '|' -f 2)
  local repo_name_2=$(cat properties_values_2 | grep repo_name | cut -d '|' -f 2)
  local schema_1=$(cat properties_values_1| grep schema | cut -d '|' -f 2)
  local schema_2=$(cat properties_values_2| grep schema | cut -d '|' -f 2)
  local schema_revision_1=$(cat properties_values_1 | grep schema_revision | cut -d '|' -f 2)
  local schema_revision_2=$(cat properties_values_2| grep schema_revision | cut -d '|' -f 2)

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


cvmfs_server_stats() {

  local output_db="output.db"   # default output file
  local db_file_1=""
  local db_file_2=""
  local merge_option=0
  local print_stats=0
  local repo_name=""
  local repo_stats=""

  
  if [ "$#" -eq 0 ]; then
    usage "No parameters given."
  fi

  while [ "$2" != "" ]; do
    case $1 in
      -o | --output )
        output_db=$2
        ;;
      -r | --repo )
        print_stats=1
        repo_name=$2
        repo_stats="/var/spool/cvmfs/$repo_name/stats.db"
        ;;
      -m | --merge )
        merge_option=1
        db_file_1=$2
        if [ "$3" != "" ]; then
          db_file_2=$3
          shift
        else
          die "Merge option needs two database files."
        fi
        ;;
    esac
    shift
  done

  if [ "$merge_option" -eq 1 ]; then
    cvmfs_server_merge_stats $db_file_1 $db_file_2 $output_db
  elif [ "$print_stats" -eq 1 ]; then
    check_multiple_repository_existence $repo_name
    if [ -e $repo_stats ]; then
      echo ".headers ON" > sql_config.txt
      echo "SELECT * from publish_statistics;" >> sql_config.txt
      sqlite3 $repo_stats < sql_config.txt
      # clean
      rm sql_config.txt
    else
      echo "No statistics database file for $repo_name."
    fi
  else
    usage "Incomplete command."
  fi
}

