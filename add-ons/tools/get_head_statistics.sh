#!/bin/sh

if [ $# -ne 1 ]; then
  echo "USAGE: $0 <repo url | local repo name>"
  exit 1
fi

repo_identifier="$1"

# this script retrieves repository statistics for a given repository with the
# following fields:
#   [ timestamp <date> <time> ]
#   [ root catalog hash ]
#   [ referenced CAS objects ]
#   [ revision number ]
#   [ # regular files ]
#   [ # directories ]
#   [ # symlinks ]
#   [ aggregated file size (bytes) ]
#   [ # chunked files ]
#   [ aggregated chunked file size (bytes) ]
#   [ # referenced chunks ]
#   [ # nested catalogs ]

root_info="$(./get_root_hash.py $repo_identifier)"
hashes="$(./get_referenced_hashes.py $repo_identifier | wc -l)"
stats="$(./get_info.py $repo_identifier)"

echo "$root_info $hashes $stats"
