#!/bin/bash

set -e

sudo nerdctl container prune -f

image="$1"
sudo nerdctl rmi "$image"

services=("containerd" "cvmfs-snapshotter")

for service in "${services[@]}"; do
    sudo systemctl stop "$service"
done

sudo umount /cvmfs/unpacked.cern.ch

sudo rm -rf "/var/lib/containerd"
sudo mkdir -p "/var/lib/containerd"
sudo chmod 755 "/var/lib/containerd"
sudo rm -rf "/var/lib/containerd-cvmfs-grpc"
sudo mkdir -p "/var/lib/containerd-cvmfs-grpc"
sudo chmod 755 "/var/lib/containerd-cvmfs-grpc"

sudo cvmfs_config reload -c

for service in "${services[@]}"; do
    sudo systemctl start "$service"
done

sudo mount -t cvmfs unpacked.cern.ch /cvmfs/unpacked.cern.ch
sudo cvmfs_config probe

sudo test -S "/run/containerd-cvmfs-grpc/containerd-cvmfs-grpc.sock"
