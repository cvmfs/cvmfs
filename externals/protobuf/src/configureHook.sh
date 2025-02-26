#!/bin/sh

### On RISC-V systems, we need to run autoreconf
### to detect the correct architecture
ISA=`grep isa /proc/cpuinfo | head -1 | cut -d: -f2`
echo "System ISA is: $ISA"
case "$ISA" in
*rv64*) autoreconf -vfi
esac
################################

CFLAGS="$CVMFS_BASE_C_FLAGS -fPIC" CXXFLAGS="$CVMFS_BASE_CXX_FLAGS -fPIC" ./configure --without-zlib --disable-shared \
    --prefix=$EXTERNALS_INSTALL_LOCATION
