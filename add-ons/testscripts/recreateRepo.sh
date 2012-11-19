#!/bin/sh

REPO=dev.cern.ch
USER=rene

echo "removing $REPO..."
echo "y" | sudo cvmfs_server rmfs $REPO

echo "recreating $REPO..."
sudo cvmfs_server mkfs $REPO -o $USER


echo
echo "done"
