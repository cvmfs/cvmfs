#!/bin/sh

export LD_LIBRARY_PATH=/usr/lib:$LD_LIBRARY_PATH
rfsctl -f cvmfsflt -c
rfsctl -f cvmfsflt -u
rmmod cvmfsflt

