#!/bin/sh

echo "compiling..."
clang++ -o spike -O3 -g spike.cc cvmfs/logging.cc -lrt -ltbb -ltbbmalloc -lz -lcrypto
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
