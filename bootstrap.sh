#!/bin/sh

set -e

CARES_VERSION=1.10.0
CURL_VERSION=7.39.0
PACPARSER_VERSION=1.3.5
ZLIB_VERSION=1.2.8
SPARSEHASH_VERSION=1.12
LEVELDB_VERSION=1.18
GOOGLETEST_VERSION=1.7.0
TBB_VERSION=4.3-1
LIBGEOIP_VERSION=1.6.0
PYTHON_GEOIP_VERSION=1.3.1

if [ $# -ne 1 ]; then
  echo "Usage: $0 <decompress location>"
  exit 1
fi

externals_build_dir="$1"
repo_root=$(pwd)
externals_dir="$repo_root/externals"

# check if bootstrapping already happened
if [ -f "$externals_build_dir/.decompressionDone" ]; then
  exit 0
fi

print_hint() {
  local msg="$1"
  echo "--> $msg"
}

get_destination_dir() {
  local library_name="$1"
  echo "$externals_build_dir/build_$library_name"
}

do_extract() {
  local library_name="$1"
  local library_archive="$2"

  local library_dir="$externals_dir/$library_name"
  local dest_dir=$(get_destination_dir $library_name)
  local cdir=$(pwd)
  local library_decompressed_dir=$(basename $library_archive .tar.gz)

  print_hint "Extracting $library_archive"

  cd $externals_build_dir
  tar xvfz "$library_dir/$library_archive"
  mv $library_decompressed_dir $dest_dir
  cd $cdir
  cp $library_dir/src/* $dest_dir
}

do_copy() {
  local library_name="$1"

  local library_dir="$externals_dir/$library_name"
  local dest_dir=$(get_destination_dir $library_name)

  print_hint "Copying $library_name"

  mkdir -p $dest_dir
  cp -r $library_dir/src/* $dest_dir
}

patch_external() {
  local library_name="$1"
  shift 1
  local cdir=$(pwd)

  print_hint "Patching $library_name"

  cd $(get_destination_dir $library_name)
  while [ $# -gt 0 ]; do
    patch -p0 < $1
    shift 1
  done
  cd $cdir
}

replace_in_external() {
  local library_name="$1"
  local src="$2"
  local dst="$3"
  local cdir=$(pwd)

  print_hint "Replacing $src with $dst in $library_name"

  cd $(get_destination_dir $library_name)
  mv "$dst" "${dst}.orig"
  cp "$src" "$dst"
  cd $cdir
}

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

mkdir -p $externals_build_dir

do_extract  "c-ares"      "c-ares-${CARES_VERSION}.tar.gz"
do_extract  "libcurl"     "curl-${CURL_VERSION}.tar.gz"
do_extract  "pacparser"   "pacparser-${PACPARSER_VERSION}.tar.gz"
do_extract  "zlib"        "zlib-${ZLIB_VERSION}.tar.gz"
do_extract  "sparsehash"  "sparsehash-${SPARSEHASH_VERSION}.tar.gz"
do_extract  "leveldb"     "leveldb-${LEVELDB_VERSION}.tar.gz"
do_extract  "googletest"  "gtest-${GOOGLETEST_VERSION}.tar.gz"
do_extract  "libgeoip"    "GeoIP-${LIBGEOIP_VERSION}.tar.gz"
do_extract  "python-geoip" "GeoIP-${PYTHON_GEOIP_VERSION}.tar.gz"
do_extract  "tbb"         "tbb-${TBB_VERSION}.tar.gz"

do_copy     "sqlite3"
do_copy     "vjson"
do_copy     "sha2"
do_copy     "sha3"

patch_external "leveldb"     "dont_search_snappy.patch"           \
                             "dont_search_tcmalloc.patch"         \
                             "arm64_memory_barrier.patch"
patch_external "pacparser"   "fix_find_proxy_ex.patch"
patch_external "tbb"         "custom_library_suffix.patch"        \
                             "symlink_to_build_directories.patch" \
                             "32bit_mock.patch"
patch_external "vjson"       "missing_include.patch"
patch_external "sparsehash"  "fix_sl4_compilation.patch"

replace_in_external "c-ares"      "config.guess.latest" "config.guess"
replace_in_external "c-ares"      "config.sub.latest" "config.sub"
replace_in_external "googletest"  "config.guess.latest" "build-aux/config.guess"
replace_in_external "googletest"  "config.sub.latest" "build-aux/config.sub"
replace_in_external "libcurl"     "config.guess.latest" "config.guess"
replace_in_external "libcurl"     "config.sub.latest" "config.sub"
replace_in_external "libgeoip"    "config.guess.latest" "config.guess"
replace_in_external "libgeoip"    "config.sub.latest" "config.sub"
replace_in_external "sparsehash"  "config.guess.latest" "config.guess"
replace_in_external "sparsehash"  "config.sub.latest" "config.sub"


# create a hint that bootstrapping is already done
touch "$externals_build_dir/.decompressionDone"
