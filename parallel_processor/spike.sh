#!/bin/sh

echo "compiling..."
LINKER_FLAGS="-ltbb -ltbbmalloc -lz -lcrypto"
if [ $(uname) = "Linux" ]; then
  LINKER_FLAGS="${LINKER_FLAGS} -lrt"
fi
clang++ -o spike -O3 -g -DNDEBUG main.cc chunk.cc util.cc file.cc processor.cc io_dispatcher.cc chunk_detector.cc ../cvmfs/logging.cc $LINKER_FLAGS
if [ $? -ne 0 ]; then
  exit 1
fi

echo "cleanup..."
rm -fR /Volumes/ramdisk/output
mkdir /Volumes/ramdisk/output
rm -fR output
mkdir output

if [ x"$1" != x"" ]; then
  echo "clear all caches..."
  echo 3 > /proc/sys/vm/drop_caches
fi

echo "- - - - - -"

./spike
