#!/bin/bash
set -euo pipefail

docker exec  cvmfs-pub1 cvmfs_server transaction
docker exec  cvmfs-pub1 bash -c 'echo abc > /cvmfs/test.repo.org/testfile'
docker exec  cvmfs-pub1 cvmfs_server publish
docker exec  cvmfs-pub1 cvmfs_server transaction
docker exec  cvmfs-pub1 bash -c 'echo important_change > /cvmfs/test.repo.org/testfile'
docker exec  cvmfs-pub2 cvmfs_server transaction -t 5000 &
sleep 1
docker exec  cvmfs-pub1 cvmfs_server publish
wait $(jobs -p) 
sleep 1
docker exec  cvmfs-pub2 bash -c 'echo unrelated_change > /cvmfs/test.repo.org/another_file'
docker exec  cvmfs-pub2 cvmfs_server publish
docker exec  cvmfs-gw1 cvmfs_server tag -l
docker exec  cvmfs-gw1 cat /cvmfs/test.repo.org/another_file
docker exec  cvmfs-gw1 cat /cvmfs/test.repo.org/testfile
docker exec  cvmfs-gw1 grep important_change /cvmfs/test.repo.org/testfile
