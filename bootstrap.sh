#!/usr/bin/env bash

set -e

SSL_VERSION=3.5.3
CRYPTO_VERSION=3.5.3
CARES_VERSION=1.18.1
CURL_VERSION=7.86.0
PACPARSER_VERSION=1.4.3
ZLIB_VERSION=1.2.8
SPARSEHASH_VERSION=1.12
LEVELDB_VERSION=1.18
GOOGLETEST_VERSION=1.8.0
IPADDRESS_VERSION=1.0.22
MAXMINDDB_VERSION=1.5.4
PROTOBUF_VERSION=2.6.1
RAPIDCHECK_VERSION=0.0
LIBARCHIVE_VERSION=3.3.2
GO_VERSION=1.18

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
echo "Bootstrap - 64bit: $IS_64_BIT"

externals_lib_dir=$EXTERNALS_LIB_LOCATION
externals_build_dir=$EXTERNALS_BUILD_LOCATION
externals_install_dir=$EXTERNALS_INSTALL_LOCATION
repo_root=$(pwd)

# set number of parallel jobs for compiling externals
export CVMFS_BUILD_EXTERNAL_NJOBS="$(getconf _NPROCESSORS_ONLN 2>/dev/null)"


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
    tar --no-same-owner -jxvf "$library_dir/$library_archive"
  else
    tar --no-same-owner -zxvf "$library_dir/$library_archive"
  fi
  mv $library_decompressed_dir $dest_dir
  cd $cdir
  cp -r $library_dir/src/* $dest_dir
}

do_extract_go() {
  local library_name="$1"
  local library_archive="$2"

  local library_dir="$externals_lib_dir/$library_name"
  local dest_dir=$(get_destination_dir $library_name)
  local cdir=$(pwd)

  print_hint "Extracting $library_archive"

  cd $externals_build_dir
  tar --no-same-owner -xvf "$library_dir/$library_archive"
  mv go $dest_dir
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
  ./configureHook.sh
  ./makeHook.sh
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

      do_extract "libcurl" "curl-${CURL_VERSION}.tar.bz2"
      patch_external "libcurl" "reenable_poll_darwin.patch"
      do_build "libcurl"
      ;;
    libcrypto)
      do_extract "libcrypto" "libressl-${CRYPTO_VERSION}.tar.gz"
      do_build "libcrypto"
      ;;
    pacparser)
      do_extract "pacparser"     "pacparser-${PACPARSER_VERSION}.tar.gz"
      patch_external "pacparser" "fix_cflags.patch"
      patch_external "pacparser" "fix_c99.patch"
      patch_external "pacparser" "fix_git_dependency.patch"
      patch_external "pacparser" "fix_python_setuptools.patch"
      patch_external "pacparser" "fix_gcc14.patch"
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
    maxminddb)
      if [ x"$BUILD_SERVER" != x ] && [ x"$BUILD_GEOAPI" != x ]; then
        do_extract "maxminddb" "MaxMind-DB-Reader-python-${MAXMINDDB_VERSION}.tar.gz"
        do_build "maxminddb"
      fi
      ;;
    protobuf)
      do_extract "protobuf"     "protobuf-${PROTOBUF_VERSION}.tar.bz2"
      patch_external "protobuf" "fix-iterator-cxx17.patch"
      do_build "protobuf"
      ;;
    googlebench)
        do_copy "googlebench"
        do_build "googlebench"
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
      patch_external "libarchive" "fix-new-glibc.patch"
      patch_external "libarchive" "libarchive_cmake.patch"
      do_build "libarchive"
      ;;
    golang)
      if [ x"$BUILD_GATEWAY" != x ] || [ x"$BUILD_DUCC" != x ] || [ x"$BUILD_SNAPSHOTTER" != x ]; then
        do_extract_go "go" "go${GO_VERSION}.src.tar.gz"
        do_build "go"
      fi
      ;;
    *)
      echo "Unknown library name. Exiting."
      exit 1
  esac
  echo $l >> $externals_install_dir/.bootstrapDone
}

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# Build a list of libs that need to be built
missing_libs="libcurl libcrypto pacparser zlib sparsehash leveldb googletest maxminddb protobuf sqlite3 vjson sha3 libarchive"

if [ x"$BUILD_UBENCHMARKS" != x"" ]; then
    missing_libs="$missing_libs googlebench"
fi


if [ x"$BUILD_QC_TESTS" != x"" ]; then
    missing_libs="$missing_libs rapidcheck"
fi
if [ x"$BUILD_GATEWAY" != x ] || [ x"$BUILD_DUCC" != x ] || [ x"$BUILD_SNAPSHOTTER" != x ]; then
    missing_libs="$missing_libs golang"
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
