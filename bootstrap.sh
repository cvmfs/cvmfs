#!/bin/sh

CARES_VERSION=1.9.1
CURL_VERSION=7.27.0
ZLIB_VERSION=1.2.7
SPARSEHASH_VERSION=1.12
LEVELDB_VERSION=1.5.0
GOOGLETEST_VERSION=1.6.0

if [ $# -ne 1 ]; then
  echo "Usage: $0 <decompress location>"
  exit 1
fi

externals_build_dir=$1
repo_root=$(pwd)
externals_dir="$repo_root/externals"

# check if bootstrapping already happened
if [ -f "$externals_build_dir/.decompressionDone" ]; then
  exit 0
fi

do_extract() {
  local library_name="$1"
  local library_archive="$2"

  local library_dir="$externals_dir/$library_name"
  local dest_dir="$externals_build_dir/$library_name"
  local cdir=$(pwd)
  local library_archive_extension
  local library_decompressed_dir

  library_archive_extension=$(echo $library_archive | sed "s/^.*\.\([a-z]*\)$/\1/")

  cd $externals_build_dir
  if [ "$library_archive_extension" = "gz" ]; then
    library_decompressed_dir=$(basename $library_archive .tar.gz)
    tar xvfz "$library_dir/$library_archive"
  elif [ "$library_archive_extension" = "zip" ]; then
    library_decompressed_dir=$(basename $library_archive .zip)
    unzip "$library_dir/$library_archive"
  else
    echo "unknown archive file extension $library_archive_extension"
    exit 2
  fi
  mv $library_decompressed_dir $library_name
  cd $cdir
  cp $library_dir/src/* $dest_dir
}

do_copy() {
  local library_name="$1"

  local library_dir="$externals_dir/$library_name"
  local dest_dir="$externals_build_dir/$library_name"

  mkdir -p $dest_dir
  cp $library_dir/src/* $dest_dir
}

patch_leveldb() {
  local cdir=$(pwd)
  cd "$externals_build_dir/leveldb/"
  patch < dont_search_snappy.patch
  cd $cdir
}

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

mkdir -p $externals_build_dir

do_extract  "c-ares"      "c-ares-${CARES_VERSION}.tar.gz"
do_extract  "libcurl"     "curl-${CURL_VERSION}.tar.gz"
do_extract  "zlib"        "zlib-${ZLIB_VERSION}.tar.gz"
do_extract  "sparsehash"  "sparsehash-${SPARSEHASH_VERSION}.tar.gz"
do_extract  "leveldb"     "leveldb-${LEVELDB_VERSION}.tar.gz"
do_extract  "googletest"  "gtest-${GOOGLETEST_VERSION}.zip"

do_copy     "sqlite3"
do_copy     "vjson"

patch_leveldb

# create a hint that bootstrapping is already done
touch "$externals_build_dir/.decompressionDone"
