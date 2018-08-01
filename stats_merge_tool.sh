#!/bin/sh

# Receive multiple release manager statistics database files and merge them
# check if they belong to the same repo

if [ "$#" -lt 2 ]; then
	echo "Usage: ./stats_merge_tool.sh stats1.db stats2.db [output.db]"
	exit 1
fi
if [ "$#" -ge 4 ]; then
	echo "Usage: ./stats_merge_tool.sh stats1.db stats2.db [output.db]"
	exit 1
fi

# default output file
output_db=output.db

if [ "$#" -eq 3 ]; then
	output_db=$3
fi

sqlite3 $1 "SELECT key, value from properties" > properties_values_1
sqlite3 $2 "SELECT key, value from properties" > properties_values_2

repo_name_1=$(cat properties_values_1 | grep repo_name | cut -d '|' -f 2)
repo_name_2=$(cat properties_values_2 | grep repo_name | cut -d '|' -f 2)
schema_1=$(cat properties_values_1| grep schema | cut -d '|' -f 2)
schema_2=$(cat properties_values_2| grep schema | cut -d '|' -f 2)
schema_revision_1=$(cat properties_values_1 | grep schema_revision | cut -d '|' -f 2)
schema_revision_2=$(cat properties_values_2| grep schema_revision | cut -d '|' -f 2)

# Sanity checks
if [ "x$repo_name_1" != "x$repo_name_2" ]; then
  echo "*** The given db files have different repo_name: $repo_name_1 vs $repo_name_2!"
  exit 1
fi
if [ "x$schema_1" != "x$schema_2" ]; then
  echo "*** The given db files have different schema: $schema_1 vs $schema_2!"
  exit 1
fi
if [ "x$schema_revision_1" != "x$schema_revision_2" ]; then
  echo "*** The given db files have different schema_revision: $schema_revision_1 vs $schema_revision_2!"
  exit 1
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
echo "" > $output_db  # make sure the output file is empty

# Merge!
sqlite3 $output_db < tmp.txt

# clean
rm tmp.txt
rm script_publish_statistics 
rm script_properties
rm properties_values_* 
