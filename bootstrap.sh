#!/bin/sh

CARES_VERSION=1.9.1
CURL_VERSION=7.27.0
ZLIB_VERSION=1.2.7
SPARSEHASH_VERSION=1.12
LEVELDB_VERSION=1.5.0
GOOGLETEST_VERSION=1.6.0

# put the extracted stuff out of source for compilation (location given by cmake)
outOfSource=$1

# check if bootstrapping already happened
if [ -f "$outOfSource/.decompressionDone" ]; then
	exit 0
fi

repo_root=$(pwd)

# C-ARES
cd externals/c-ares
tar xvfz c-ares-${CARES_VERSION}.tar.gz
mkdir -p "$outOfSource/c-ares/src"
mv c-ares-${CARES_VERSION}/* "$outOfSource/c-ares/src"
cp src/* "$outOfSource/c-ares/src"
rm -rf c-ares-${CARES_VERSION}
cd $repo_root

# CURL
cd externals/libcurl
tar xfz curl-${CURL_VERSION}.tar.gz
mkdir -p "$outOfSource/libcurl/src"
mv curl-${CURL_VERSION}/* "$outOfSource/libcurl/src"
cp src/* "$outOfSource/libcurl/src"
rm -rf curl-${CURL_VERSION}
cd $repo_root

# Zlib
cd externals/zlib
tar xfz zlib-${ZLIB_VERSION}.tar.gz
mkdir -p "$outOfSource/zlib/src"
mv zlib-${ZLIB_VERSION}/* "$outOfSource/zlib/src"
cp src/* "$outOfSource/zlib/src"
rm -rf zlib-${ZLIB_VERSION}
cd $repo_root

# sqlite3
cd externals/sqlite3
mkdir -p "$outOfSource/sqlite3/src"
cp src/* "$outOfSource/sqlite3/src"
cd $repo_root

# vjson
cd externals/vjson
mkdir -p "$outOfSource/vjson/src"
cp src/* "$outOfSource/vjson/src"
cd $repo_root

# google sparse hash
cd externals/sparsehash
tar xfz sparsehash-${SPARSEHASH_VERSION}.tar.gz
mkdir -p "$outOfSource/sparsehash/src"
mv sparsehash-${SPARSEHASH_VERSION}/* "$outOfSource/sparsehash/src"
cp src/* "$outOfSource/sparsehash/src"
rm -rf sparsehash-${SPARSEHASH_VERSION}
cd $repo_root

# leveldb
cd externals/leveldb
tar xvfz leveldb-${LEVELDB_VERSION}.tar.gz
mkdir -p "$outOfSource/leveldb/src"
mv leveldb-${LEVELDB_VERSION}/* "$outOfSource/leveldb/src"
cp src/* "$outOfSource/leveldb/src"
cd "$outOfSource/leveldb/src" && patch < dont_search_snappy.patch
rm -rf leveldb-${LEVELDB_VERSION}
cd $repo_root

# googletest
cd externals/googletest
unzip gtest-${GOOGLETEST_VERSION}.zip
mkdir -p "$outOfSource/googletest/src"
mv gtest-${GOOGLETEST_VERSION}/* "$outOfSource/googletest/src"
cp src/* "$outOfSource/googletest/src"
rm -rf gtest-${GOOGLETEST_VERSION}
cd $repo_root

# create a hint that bootstrapping is already done
touch "$outOfSource/.decompressionDone"
