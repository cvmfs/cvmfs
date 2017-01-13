#!/bin/sh

sh configure CFLAGS="$CVMFS_EXTERNAL_CXX_FLAGS -fPIC" --with-pic
