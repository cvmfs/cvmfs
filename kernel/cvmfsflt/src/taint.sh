#!/bin/sh

export LD_LIBRARY_PATH=/usr/lib:$LD_LIBRARY_PATH
make
sync
modprobe redirfs
insmod ./cvmfsflt.ko
rfsctl -f cvmfsflt -i /tmp/mount

