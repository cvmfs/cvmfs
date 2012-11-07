#!/bin/sh

REPO=dev.cern.ch
USER=rene

echo "removing $REPO..."
echo "y" | sudo cvmfs_server rmfs $REPO

echo "recreating $REPO..."
echo $USER | sudo cvmfs_server mkfs $REPO


echo
echo "done"
