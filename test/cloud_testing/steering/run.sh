#!/bin/sh

# This script spawns a virtual machine of a specific platform type on ibex and
# runs the associated test cases on this machine

# TODO: put this configuration separate
EC2_ACCESS_KEY="074d214e8f884d93a3efe71ad0640d4b"
EC2_SECRET_KEY="f4fe3774b1d845ffbeede409ec664cdc"
EC2_KEY="ibex_key"
EC2_INSTANCE_TYPE="m1.medium"
EC2_KEY_LOCATION="/home/rene/ibex/ibex_key.pem"

nightly_url="http://ecsft.cern.ch/dist/cvmfs/nightlies/cvmfs-git-207-61c90be131a89b7b"
script_location=$(dirname $(readlink --canonicalize $0))

ip_address=""
instance_id=""

die() {
  local msg=$1
  echo $msg
  exit 103
}


spawn_virtual_machine() {
  local ami=$1

  # spawn the virtual machine on ibex
  echo -n "spawning virtual machine from $ami... "
  local spawn_results
  spawn_results=$(${script_location}/instance_handler.py spawn                 \
                                         --access-key       $EC2_ACCESS_KEY    \
                                         --secret-key       $EC2_SECRET_KEY    \
                                         --key              $EC2_KEY           \
                                         --instance-type    $EC2_INSTANCE_TYPE \
                                         --ami              $ami)
  if [ $? -ne 0 ]; then
    echo "fail"
    return 1
  else
    instance_id=$(echo $spawn_results | awk '{print $1}')
    ip_address=$(echo $spawn_results | awk '{print $2}')
    echo "okay"
  fi

  # wait for the virtual machine to respond to pings
  echo -n "waiting for virtual machine to become reachable... "
  local timeout=30
  while [ $timeout -gt 0 ] && ! ping -c 1 $ip_address > /dev/null 2>&1; do
    sleep 10
    timeout=$(( $timeout - 1 ))
  done
  if [ $timeout -eq 0 ]; then
    echo "fail"
    return 1
  else
    echo "okay"
  fi

  # wait for the virtual machine to become accessible via ssh
  echo -n "waiting for virtual machine to become accessible... "
  timeout=30
  while [ $timeout -gt 0 ] && \
        ! ssh -i $EC2_KEY_LOCATION -o StrictHostKeyChecking=no root@$ip_address 'echo hallo' > /dev/null 2>&1; do
    sleep 10
    timeout=$(( $timeout - 1 ))
  done
  if [ $timeout -eq 0 ]; then
    echo "fail"
    return 1
  else
    echo "okay"
  fi

  # all done...
  return 0
}


slc5_i386() {
  spawn_virtual_machine "ami-00000149" || exit 1

  return 0
  #ssh -i ~/ibex/ibex_key.pem root@188.184.133.28 'cat | bash /dev/stdin -s http://ecsft.cern.ch/dist/cvmfs/nightlies/cvmfs-git-206-f98ab56109e7d489/cvmfs-server-2.1.10-0.206.f98ab56109e7d489git.el5.x86_64.rpm -c http://ecsft.cern.ch/dist/cvmfs/nightlies/cvmfs-git-206-f98ab56109e7d489/cvmfs-2.1.10-0.206.f98ab56109e7d489git.el5.x86_64.rpm -t http://ecsft.cern.ch/dist/cvmfs/nightlies/cvmfs-git-206-f98ab56109e7d489/cvmfs-git-f98ab56109e7d489.tar.gz -k http://ecsft.cern.ch/dist/cvmfs/cvmfs-keys/cvmfs-keys-1.4-1.noarch.rpm -r slc5.sh' -p /home/sftnight/scripts < run.sh
}


# check if the platform parameter was given
if [ $# -ne 1 ]; then
  echo "Usage: $0 <platform => slc5_i386 | slc5_x86_64 | slc6_i386 | slc6_x86_64>"
  exit 1
fi

#amis="ami-00000149 ami-00000147 ami-0000014a ami-00000148"
# find out if we got a valid platform descriptor
target_platform=$1
case $target_platform in
  slc5_i386)
    slc5_i386
    ;;
  slc5_x86_64)
    ;;
  slc6_i386)
    ;;
  slc6_x86_64)
    ;;
  *)
    echo "Unknown platform $target_platform"
    ;;
esac
