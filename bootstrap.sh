#!/bin/sh

set -e

SSL_VERSION=3.1.2
CARES_VERSION=1.16.1
CURL_VERSION=7.71.1
PACPARSER_VERSION=1.3.5
ZLIB_VERSION=1.2.8
SPARSEHASH_VERSION=1.12
LEVELDB_VERSION=1.18
GOOGLETEST_VERSION=1.8.0
IPADDRESS_VERSION=1.0.22
MAXMINDDB_VERSION=1.5.4
PROTOBUF_VERSION=2.6.1
RAPIDCHECK_VERSION=0.0
LIBARCHIVE_VERSION=3.3.2

if [ x"$EXTERNALS_LIB_LOCATION" = x"" ]; then
  echo "Bootstrap - Missing environment variable: EXTERNALS_LIB_LOCATION"
  exit 1;
fi
if [ x"$EXTERNALS_BUILD_LOCATION" = x"" ]; then
  echo "Bootstrap - Missing environment variable: EXTERNALS_BUILD_LOCATION"
 exit 1;
fi
if [ x"$EXTERNALS_INSTALL_LOCATION" = x"" ]; then
  echo "Bootstrap - Missing environment variable: EXTERNALS_INSTALL_LOCATION"
 exit 1;
fi

echo "Bootstrap - Externals lib location: $EXTERNALS_LIB_LOCATION"
echo "Bootstrap - Externals build location: $EXTERNALS_BUILD_LOCATION"
echo "Bootstrap - Externals install location: $EXTERNALS_INSTALL_LOCATION"
echo "Bootstrap - Base CVMFS C flags: $CVMFS_BASE_C_FLAGS"
echo "Bootstrap - Base CVMFS C++ flags: $CVMFS_BASE_CXX_FLAGS"

externals_lib_dir=$EXTERNALS_LIB_LOCATION
externals_build_dir=$EXTERNALS_BUILD_LOCATION
externals_install_dir=$EXTERNALS_INSTALL_LOCATION
repo_root=$(pwd)



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
  local archive_format=$(echo "$library_archive" | sed 's/.*\(\.tar\.[^\.]*\)$/\1/')

  local library_dir="$externals_lib_dir/$library_name"
  local dest_dir=$(get_destination_dir $library_name)
  local cdir=$(pwd)
  local library_decompressed_dir=$(basename $library_archive $archive_format)

  print_hint "Extracting $library_archive"

  cd $externals_build_dir
  if [ $archive_format = ".tar.bz2" ]; then
    tar xvfj "$library_dir/$library_archive"
  else
    tar xvfz "$library_dir/$library_archive"
  fi
  mv $library_decompressed_dir $dest_dir
  cd $cdir
  cp -r $library_dir/src/* $dest_dir
}

do_copy() {
  local library_name="$1"

  local library_dir="$externals_lib_dir/$library_name"
  local dest_dir=$(get_destination_dir $library_name)

  print_hint "Copying $library_name"

  mkdir -p $dest_dir
  cp -r $library_dir/src/* $dest_dir
}

do_build() {
  local library_name="$1"
  local library_src_dir="$externals_lib_dir/$library_name"
  local library_build_dir=$(get_destination_dir $library_name)

  print_hint "Building $library_name"

  local save_dir=$(pwd)
  cd $library_build_dir
  sh configureHook.sh
  sh makeHook.sh
  cd $save_dir
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

build_lib() {
  local lib_name=$1
  rm -rf $externals_build_dir/build_${lib_name}
  case ${lib_name} in
    libcurl)
      # (only on Mac)
      if [ x"$(uname)" = x"Darwin" ]; then
          rm -rf $externals_build_dir/build_ssl
          do_extract "ssl" "libressl-${SSL_VERSION}.tar.gz"
          do_build "ssl"
      fi

      rm -rf $externals_build_dir/build_c-ares
      do_extract "c-ares" "c-ares-${CARES_VERSION}.tar.gz"
      do_build "c-ares"

      do_extract "libcurl" "curl-${CURL_VERSION}.tar.gz"
      patch_external "libcurl" "reenable_poll_darwin.patch"
      do_build "libcurl"
      ;;
    pacparser)
      do_extract "pacparser"    "pacparser-${PACPARSER_VERSION}.tar.gz"
      patch_external "pacparser"   "fix_find_proxy_ex.patch" \
                                   "fix_cflags.patch"        \
                                   "fix_python.patch"
      do_build "pacparser"
      ;;
    zlib)
      do_extract "zlib"         "zlib-${ZLIB_VERSION}.tar.gz"
      do_build "zlib"
      ;;
    sparsehash)
      do_extract "sparsehash"   "sparsehash-${SPARSEHASH_VERSION}.tar.gz"
      patch_external "sparsehash"  "fix_sl4_compilation.patch"          \
                                  "fix_warning_gcc48.patch"
      replace_in_external "sparsehash"  "config.guess.latest" "config.guess"
      replace_in_external "sparsehash"  "config.sub.latest" "config.sub"
      do_build "sparsehash"
      ;;
    leveldb)
      do_extract "leveldb"      "leveldb-${LEVELDB_VERSION}.tar.gz"
      patch_external "leveldb"     "dont_search_snappy.patch"           \
                                  "dont_search_tcmalloc.patch"         \
                                  "arm64_memory_barrier.patch"
      do_build "leveldb"
      ;;
    googletest)
        do_extract "googletest"   "googletest-release-${GOOGLETEST_VERSION}.tar.gz"
        patch_external "googletest"     "cmake_compatibility.patch"
        do_build "googletest"
      ;;
    ipaddress)
      if [ x"$BUILD_SERVER" != x ] && [ x"$BUILD_GEOAPI" != x ]; then
        do_extract "ipaddress" "ipaddress-${IPADDRESS_VERSION}.tar.gz"
        do_build "ipaddress"
      fi
      ;;
    maxminddb)
      if [ x"$BUILD_SERVER" != x ] && [ x"$BUILD_GEOAPI" != x ]; then
        do_extract "maxminddb" "MaxMind-DB-Reader-python-${MAXMINDDB_VERSION}.tar.gz"
        do_build "maxminddb"
      fi
      ;;
    protobuf)
      do_extract "protobuf"     "protobuf-${PROTOBUF_VERSION}.tar.bz2"
      do_build "protobuf"
      ;;
    googlebench)
      if [ x"$BUILD_UBENCHMARKS" != x"" ]; then
        do_copy "googlebench"
        do_build "googlebench"
      fi
      ;;
    sqlite3)
      do_copy "sqlite3"
      do_build "sqlite3"
      ;;
    vjson)
      do_copy "vjson"
      patch_external "vjson"       "missing_include.patch"
      do_build "vjson"
      ;;
    sha2)
      do_copy "sha2"
      do_build "sha2"
      ;;
    sha3)
      do_copy "sha3"
      do_build "sha3"
      ;;
    rapidcheck)
      if [ x"$BUILD_QC_TESTS" != x"" ]; then
        do_extract "rapidcheck" "rapidcheck-${RAPIDCHECK_VERSION}.tar.gz"
        do_build "rapidcheck"
      fi
      ;;
    libarchive)
      do_extract "libarchive" "libarchive-${LIBARCHIVE_VERSION}.tar.gz"
      do_build "libarchive"
      ;;
    *)
      echo "Unknown library name. Exiting."
      exit 1
  esac
  echo $l >> $externals_install_dir/.bootstrapDone
}

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# Build a list of libs that need to be built
missing_libs="libcurl pacparser zlib sparsehash leveldb googletest ipaddress maxminddb protobuf googlebench sqlite3 vjson sha2 sha3 libarchive"
if [ x"$BUILD_QC_TESTS" != x"" ]; then
    missing_libs="$missing_libs rapidcheck"
fi

if [ -f $externals_install_dir/.bootstrapDone ]; then
  existing_libs=$(cat $externals_install_dir/.bootstrapDone)
  for l in $existing_libs; do
    if [ x"$l" != x ]; then
      echo "Bootstrap - found $l"
      missing_libs=$(echo $missing_libs | sed -e "s/$l//")
    fi
  done
else
  echo "Bootstrap - clean build"
fi

mkdir -p $externals_build_dir
mkdir -p $externals_install_dir/include
mkdir -p $externals_install_dir/lib

rm -f $externals_install_dir/.bootstrapDone
for l in $existing_libs; do
  echo $l >> $externals_install_dir/.bootstrapDone
done

if [ x"$missing_libs" != x ]; then
  echo "Building libraries: $missing_libs"
fi

for l in $missing_libs; do
  build_lib $l
done

## Done!
