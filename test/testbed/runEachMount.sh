#!/bin/bash

repo_name=$1

fid=$(stat $repo_name | grep "Device" | cut -d'/' -f 2 | cut -d'd' -f 1)

echo "Repo $1 has bdi 0:$fid"
echo -n "Set read_ahead_kb to "

echo 1024 | sudo tee /sys/class/bdi/0:"$fid"/read_ahead_kb