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
IPADDRESS_VERSION=1.0.22
MAXMINDDB_VERSION=1.5.4
PROTOBUF_VERSION=2.6.1
RAPIDCHECK_VERSION=0.0
LIBARCHIVE_VERSION=3.3.2
GOLANG_VERSION=1.24.2

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

# Helper function to remove a word from a space-separated list
# Works consistently across macOS (BSD sed) and Linux (GNU sed)
remove_word_from_list() {
  local list="$1"
  local word_to_remove="$2"

  # Use a more portable approach that works on both BSD and GNU sed
  # First, handle the word at the beginning of the list
  list=$(echo "$list" | sed "s/^$word_to_remove //" | sed "s/^$word_to_remove$//")
  # Then handle the word in the middle or end of the list
  list=$(echo "$list" | sed "s/ $word_to_remove / /g" | sed "s/ $word_to_remove$//")
  # Clean up any extra spaces
  list=$(echo "$list" | sed 's/  */ /g' | sed 's/^ *//' | sed 's/ *$//')

  echo "$list"
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

do_download_go() {
  echo "Downloading Go Binaries ..."
  mkdir -p "$externals_build_dir/build_golang_rev2"
  cd "$externals_build_dir/build_golang_rev2"
  arch=$(arch)
  goarch=""
  if [ "$arch" = "x86_64" ]; then
    goarch="amd64"
  elif [ "$arch" = "aarch64" ]; then
    goarch="arm64"
  fi
  if [ -z "${goarch}" ]; then
    >&2 echo "Error: Failed to bootstrap go, couldn't parse architecture. Install go toolchain > 1.23 manually, see https://go.dev/doc/install"
    exit 1
  fi
  echo "Downloading https://go.dev/dl/go${GOLANG_VERSION}.linux-${goarch}.tar.gz ..."
  curl -LO https://go.dev/dl/go${GOLANG_VERSION}.linux-${goarch}.tar.gz
  if [ $? -ne 0 ] ; then
   >&2 echo "Error: Failed to download go binaries! Install go toolchain > 1.23 manually, see https://go.dev/doc/install"
  fi
  mkdir -p $externals_install_dir/go
  tar -C $externals_install_dir/ -xzf go${GOLANG_VERSION}.linux-${goarch}.tar.gz
  cp -r $externals_lib_dir/golang_rev2/src/* ./
  cd -
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
      golang_rev2)
        do_download_go
        do_build "golang_rev2"
      ;;
    *)
      echo "Unknown library name. Exiting."
      exit 1
  esac
  echo $l >> $externals_install_dir/.bootstrapDone
}

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# Build a list of libs that need to be built
# Check if BUILTIN_EXTERNALS_LIST is set and override missing_libs
if [ x"$BUILTIN_EXTERNALS_LIST" != x"" ]; then
    # Convert semicolon-separated list to space-separated
    missing_libs=$(echo "$BUILTIN_EXTERNALS_LIST" | tr ';' ' ')
    echo "Bootstrap - Using custom externals list: $missing_libs"
else
    missing_libs="libcurl libcrypto pacparser zlib sparsehash leveldb maxminddb protobuf sqlite3 vjson sha3 libarchive"

    if [ x"$BUILD_UBENCHMARKS" != x"" ]; then
        missing_libs="$missing_libs googlebench"
    fi

    if [ x"$BUILD_GATEWAY" != x ] || [ x"$BUILD_DUCC" != x ] || [ x"$BUILD_SNAPSHOTTER" != x ]; then
        required_go_minor_version="23"
        if [ -n "$(command -v go)" ]; then
          go_minor_version=`go version | { read _ _ v _; echo ${v#go}; } | cut -d '.' -f2`
           if expr "'$go_minor_version" \< "'$required_go_minor_version"   > /dev/null ; then
             missing_libs="$missing_libs golang_rev2"
           fi
        else
          missing_libs="$missing_libs golang_rev2"
        fi
    fi

    if [ x"$BUILD_QC_TESTS" != x"" ]; then
        missing_libs="$missing_libs rapidcheck"
    fi

    echo "Bootstrap - Using default externals list: $missing_libs"
fi

# Apply exclusions if BUILTIN_EXTERNALS_EXCLUDE is set
if [ x"$BUILTIN_EXTERNALS_EXCLUDE" != x"" ]; then
    # Convert semicolon-separated list to space-separated
    exclude_libs=$(echo "$BUILTIN_EXTERNALS_EXCLUDE" | tr ';' ' ')
    echo "Bootstrap - Excluding libraries: $exclude_libs"

    # Remove each excluded library from missing_libs
    for exclude_lib in $exclude_libs; do
        missing_libs=$(remove_word_from_list "$missing_libs" "$exclude_lib")
    done

    # Clean up any leading/trailing spaces
    missing_libs=$(echo "$missing_libs" | sed 's/^[[:space:]]*//' | sed 's/[[:space:]]*$//')

    echo "Bootstrap - Final externals list after exclusions: $missing_libs"
fi

if [ -f $externals_install_dir/.bootstrapDone ]; then
  existing_libs=$(cat $externals_install_dir/.bootstrapDone)
  for l in $existing_libs; do
    # cleanup dependencies that are updated or no longer vendored
    if [ "$l" = "golang" ]; then
      existing_libs=$(remove_word_from_list "$existing_libs" "golang")
      if [ -d $externals_install_dir/go ]; then
        rm -rf $externals_install_dir/go
      fi
    elif [ "$l" = "googletest" ]; then
      existing_libs=$(remove_word_from_list "$existing_libs" "googletest")
      if [ -d $externals_install_dir/include/gtest ]; then
        rm -rf $externals_install_dir/include/gtest
      fi
      if [ -f $externals_install_dir/lib/libgtest.a ]; then
        rm $externals_install_dir/lib/libgtest.a
      fi
      if [ -f $externals_install_dir/lib/libgtest_main.a ]; then
        rm $externals_install_dir/lib/libgtest_main.a
      fi
    elif [ x"$l" != x ]; then
      echo "Bootstrap - found $l"
      missing_libs=$(remove_word_from_list "$missing_libs" "$l")
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
