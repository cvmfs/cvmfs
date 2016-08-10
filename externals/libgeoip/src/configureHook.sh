#!/bin/sh

sh configure CFLAGS="$(echo $CVMFS_BASE_CXX_FLAGS | sed s/-fvisibility=hidden//) -fPIC" --with-pic
