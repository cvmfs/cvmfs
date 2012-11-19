#!/bin/sh

REPO=/cvmfs/dev.cern.ch

die() {
  echo $1 >&2
  exit 1
}

echo "recreating plain repository"
sh recreateRepo.sh

echo "clearing repository"
cvmfs_server transaction || die "failed to init transaction"
rm -fR $REPO/*
rm -fR $REPO/\.*
cvmfs_server publish || die "failed to publish empty repository"

echo "all done"
