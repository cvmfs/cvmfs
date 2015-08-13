#!/bin/bash

# link the cvmfs source tree conveniently into the home directory
[ -L cvmfs ] || ln -s /vagrant cvmfs

# link /var/spool/cvmfs into the bare r/w mount of CernVM
CVMFS_SPOOL_DIR=/var/spool/cvmfs
CVMFS_SPOOL_IMPOSTER=/mnt/.rw/cvmfs_server
mkdir -p $CVMFS_SPOOL_IMPOSTER
[ ! -d $CVMFS_SPOOL_DIR ] || rm -fR $CVMFS_SPOOL_DIR
[ -L $CVMFS_SPOOL_DIR   ] || ln -s $CVMFS_SPOOL_IMPOSTER $CVMFS_SPOOL_DIR
