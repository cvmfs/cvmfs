#!/bin/sh

LOCH=`find cvmfs/src cvmfsd/src kernel/cvmfsflt/src/cvmfsflt.h -name "[A-Za-z0-9]*.h" | xargs wc -l | tail -1 | awk '{print $1}'`
LOCC=`find cvmfs/src cvmfsd/src kernel/cvmfsflt/src -name "[A-Za-z0-9]*.c" | xargs wc -l | tail -1 | awk '{print $1}'`
LOCCC=`find cvmfs/src cvmfsd/src -name "[A-Za-z0-9]*.cc" | xargs wc -l | tail -1 | awk '{print $1}'`
LOCSH=`find cvmfs/src/cvmfs-talk \
  cvmfs/src/cvmfs_decrypt \
  cvmfs/src/cvmfs_mkkey \
  cvmfs/src/cvmfs_config \
  cvmfsd/cvmfs-sync \
  cvmfsd/cvmfsd.initd \
  mount/auto.cvmfs \
  mount/config.sh \
  mount/cvmfs.initd \
  mount/mount.cvmfs \
  mount/umount.cvmfs \
  keys/*.pl \
  | xargs wc -l | tail -1 | awk '{print $1}'`
LOCAM=`wc -l configure.ac Makefile.am bootstrap.sh \
  rpm/cvmfs.spec rpm/cvmfs-keys.spec rpm/cvmfs-server.spec rpm/cvmfs-auto-setup.spec \
  rpm/cvmfsflt.spec rpm/redirfs.spec rpm/cvmfs-init-scripts.spec \
  cvmfs/Makefile.am cvmfs/src/Makefile.am \
  cvmfsd/Makefile.am cvmfsd/src/Makefile.am \
  kernel/Makefile kernel/cvmfsflt/Makefile kernel/redirfs/Makefile \
  libcurl/Makefile libcurl/src/configure.gnu \
  libfuse/Makefile libfuse/src/configure.gnu \
  mount/Makefile.am \
  sqlite3/Makefile.am sqlite3/src/Makefile.am \
  | tail -1 | awk '{print $1}'`
LOCTEST=`wc -l test/*.sh test/src/*.sh | tail -1 | awk '{print $1}'`

echo "Headers: $LOCH"
echo "C code: $LOCC"
echo "C++ code: $LOCCC"
echo "Scripts: $LOCSH"
echo "Build system: $LOCAM"
echo "Total: $[$LOCH+$LOCC+$LOCCC+$LOCSH+$LOCAM]"
echo "Tests: $LOCTEST"
