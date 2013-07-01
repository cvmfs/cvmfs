#!/bin/sh

ramdisk_name="ramdisk"

clang++ -o spike -O0 -g -std=c++11 -stdlib=libc++ -ltbb -ltbbmalloc -lz -lcrypto spike.cc cvmfs/logging.cc
if [ $? -ne 0 ]; then
  exit 1
fi

umount /Volumes/$ramdisk_name
diskutil erasevolume HFS+ "$ramdisk_name" `hdiutil attach -nomount ram://4388608`

purge

echo "- - - - - -"

./spike
