#!/usr/bin/bash

# usage: ./benchmark.sh [OPTION]
#     -o outdir
#     -t testname
#     -k kcachemode
#     -c cachemode
#     -b backend
#     -d debuglog
#     -p     # run under parrot
#     -x     # set -x
#
# This script should be run from the test/ directory at the root of the
# CVMFS repo. Also, you need a squid running on localhost:3128
#
# backend can be
# - fuse
# - parrot
# kcachemode can be
# - kcache
# - nokcache
# and cachemode can be
# - posix
# - ram_malloc
# - ram_arena

. bench_common.sh

set -e

while getopts ':o:t:k:c:d:b:x' opt; do
  case $opt in
  o)
    outdir="$OPTARG"
    ;;
  t)
    testname="$OPTARG"
    ;;
  k)
    kcache="$OPTARG"
    ;;
  c)
    cachemode="$OPTARG"
    ;;
  d)
    debuglog=$(readlink -f "$OPTARG")
    ;;
  b)
    backend="$OPTARG"
    ;;
  x)
    set -x
    ;;
  \?)
    die invalid option "$OPTARG"
    ;;
  :)
    die missing argument to "$OPTARG"
    ;;
  esac
done

if [ "$kcache" = "kcache" ]; then
  unset kcache_timeout
elif [ "$kcache" = "nokcache" ]; then
  kcache_timeout=-1
else
  die bad kcache
fi
if [ "$cachemode" = "posix" ]; then
  cache=posix
  unset malloc
elif [ "$cachemode" = "ram_malloc" ]; then
  cache=ram
  malloc=libc
elif [ "$cachemode" = "ram_arena" ]; then
  cache=ram
  malloc=arena
else
  die bad cache mode
fi
if [ "$backend" != "fuse" ] && [ "$backend" != "parrot" ]; then
  die bad backend
fi
if [ -z "$outdir" ]; then
  die no output directory
fi
if [ -e "$outdir" ]; then
  echo output directory already exists
  exit 0
fi
if [ -z "$testname" ]; then
  die no testname
fi
if ! [ -z $(find /tmp -maxdepth 1 -name '*.cern.ch.log.*') ]; then
  die /tmp is dirty
fi

mkdir -p "$outdir"
outdir=$(readlink -f "$outdir")

cat > "$outdir/benchmark.conf" <<EOF
# $(git log -n 1 --pretty=oneline)

CVMFS_RELOAD_SOCKETS=$outdir/.cvmfs_cache
CVMFS_SERVER_URL=http://cvmfs-stratum-one.cern.ch/cvmfs/@fqrn@
CVMFS_SHARED_CACHE=no
CVMFS_HTTP_PROXY=http://localhost:3128
CVMFS_AUTO_UPDATE=false
CMS_LOCAL_SITE=$(readlink -f benchmarks/004-cms)
CVMFS_QUOTA_LIMIT=-1
CVMFS_KEYS_DIR=/etc/cvmfs/keys/cern.ch
${debuglog:+CVMFS_DEBUGLOG=$debuglog}
${kcache_timeout:+CVMFS_KCACHE_TIMEOUT=$kcache_timeout}
CVMFS_CACHE_PRIMARY=${cache:-posix}
CVMFS_CACHE_RAM_MALLOC=${malloc:-libc}
CVMFS_CACHE_RAM_SIZE=50%
#CVMFS_WORKSPACE=$outdir/.workspace
#CVMFS_RELOAD_SOCKETS=$outdir
EOF

if [ "$backend" = "fuse" ]; then
  echo "CVMFS_CACHE_BASE=$outdir/.cvmfs_cache" >> "$outdir/benchmark.conf"
fi

export CVMFS_OPT_CACHEDIR=$outdir/.cvmfs_cache \
       CVMFS_OPT_WARM_CACHE=no \
       CMVFS_OPT_HOT_CACHE=yes \
       CVMFS_OPT_VALGRIND=no \
       CVMFS_TEST_SCRATCH=$outdir/.scratch \
       CVMFS_OPT_OUTPUT_DIR=$outdir/data \
       CVMFS_OPT_CONFIG_FILE=$outdir/benchmark.conf \
       PARROT_ENABLED=$parrot
if [ "$backend" = "fuse" ]; then
  ./run.sh "$outdir/test.log" "benchmarks/$testname"
else
  if [ "$testname" = "002-lhcb" ] || [ "$kcache" = "kcache" ]; then
    exit 0
  fi
# sudo -E gdb ./parrot_run
  sudo -E ./parrot_run --fake-setuid --cvmfs-repo-switching --cvmfs-option-file "$outdir/benchmark.conf" -m test.mount -- ./run.sh "$outdir/test.log" "benchmarks/$testname"
fi

find /tmp -maxdepth 1 -name '*.cern.ch.log.*' | xargs -t -I {} -- sudo mv {} "$outdir"
sudo chown -R --reference "$outdir" "$outdir"
find "$outdir/data" -mindepth 2 -maxdepth 2 | xargs -t -I {} -- mv {} "$outdir/data"
find "$outdir" -type d -name .cvmfs_cache | xargs -I {} -t -- rm -rf {}
