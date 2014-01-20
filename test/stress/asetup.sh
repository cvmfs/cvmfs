#!/bin/sh

CPUS=$(cat /proc/cpuinfo | grep -c "processor")
export ATL_LOCAL_ROOT=/cvmfs/atlas.cern.ch/repo
export ATLAS_LOCAL_ROOT_BASE=${ATL_LOCAL_ROOT}/ATLASLocalRootBase
PLATFORM=x86_64-slc5-gcc43-opt
RELEASE_BASE=${ATL_LOCAL_ROOT}/sw/software/${PLATFORM}

select_random() {
  find ${RELEASE_BASE} -maxdepth 1 -name '1*' -type d -exec basename {} \; > releases
  local num_releases=$(cat releases | wc -l)
  local pick=$(($RANDOM % $num_releases + 1))
  head -n $pick releases | tail -n1
  rm -f releases
}


stress_single() {
  id=$1

  echo "[$id] Start stress test"
  rm -f .stopped
  rm -f benchmark
  while [ ! -f .stop ]; do
    rm -f .asetup.save
    local next_release=$(select_random)
    echo "[$id] Setup release $next_release"
    local start_time=`date -u +%s`
    sh -c "source ${RELEASE_BASE}/${next_release}/cmtsite/asetup.sh ${next_release},notest --cmtconfig ${PLATFORM}" > /dev/null 2>&1
    local end_time=`date -u +%s`
    if [ ! -f .asetup.save ]; then
      echo "ERROR: failure while setup release ${RELEASE_BASE}/${next_release}" 1>&2
    else
      local duration=$(($end_time - $start_time))
      echo "$duration ($next_release)" >> benchmark
    fi
  done
  echo "[$id] Stopped"
  rm -f .stop
  touch .stopped
  exit 0
}


mkId() {
  local num=$1
  echo "CPU-$(printf "%02d" $i)"
}

echo "Found $CPUS CPUs"
echo "Run in $RELEASE_BASE"

for i in $(seq 1 $CPUS); do
  id=$(mkId $i)
  rm -rf "$id"
  mkdir $id
  cd $id
  stress_single $id &
  cd ..
done

while [ ! -f .stop ]; do
  sleep 1
done

echo "STOPPING"
for i in $(seq 1 $CPUS); do
  touch "$(mkId $i)"/.stop
done
for i in $(seq 1 $CPUS); do
  while [ ! "$(mkId $i)"/.stopped ]; do
    sleep 1
  done
done
echo "STOPPED"
rm .stop

