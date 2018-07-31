#!/bin/sh

# Usage: stats_merge_tool.sh <stats1.db> <stats2.db>

# Receive multiple release manager statistics database files and merge them
# check if they belong to the same repo
repo_name_1=$(sqlite3 $1 "SELECT key, value from properties" | grep repo_name | cut -d '|' -f 2)
repo_name_2=$(sqlite3 $2 "SELECT key, value from properties" | grep repo_name | cut -d '|' -f 2)
if [ "x$repo_name_1" != "x$repo_name_2" ]; then
  echo "*** The given db files have different repo_name: $repo_name_1 vs $repo_name_2!"
  exit 1
fi

sqlite3 $1 .dump > tmp.db
sqlite3 $2 .dump >> tmp.db
sqlite3 new.db < tmp.db

rm tmp.db
