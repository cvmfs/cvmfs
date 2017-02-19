#!/bin/sh

make -f Makefile.cvmfs clean
make CVMFS_BASE_C_FLAGS="$CVMFS_BASE_C_FLAGS -DNO_CGI -DNO_SSL" -f Makefile.cvmfs
strip -S libmongoose.a

cp -rv mongoose.h $EXTERNALS_INSTALL_LOCATION/include/
cp -rv libmongoose.a $EXTERNALS_INSTALL_LOCATION/lib/
