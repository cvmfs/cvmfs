#!/bin/bash

display_help()
{
    echo -e "Generate flamegraphs for a process that uses a given cvmfs repo."
    echo -e "This script may generate some inaccurate call traces when CVMFS runs multithreaded.\n"
    echo -e "Usage: ./generate_flamegraphs.sh <cvmfs_repo_name> [--options]\n\n"
    echo -e "Available options:\n"
    echo -e \
		 "--benchmark <benchmark_name>    Profile cvmfs while running a fixed benchmark.\n"
    echo -e \
		 "--sleep <number of seconds>     Profile the cvmfs process for a fixed time interval.\n" \
         "                               To be used as an alternative to --benchmark.\n"
    echo -e \
		 "--type <oncpu / offcpu>         Mention if the flamegraph should represent oncpu time or blocked time.\n" \
         "                               By default, oncpu flamegraphs are generated.\n"
    echo -e \
		 "--cache <cold / warm / hot>     Set the cache state while running a benchmark.\n" \
         "                               By default, the cache is hot (no change on the current cache).\n" \
		 "                               To be used together with --benchmark.\n"
    echo -e \
		 "--dwarf                         Use perf with dwarf option for profiling.\n" \
         "                               This guarantees that all the symbols in the flamegraphs are complete,\n" \
         "                               but consumes significantly more resources.\n"
    echo -e \
		 "--help                          Display this message and exit.\n"
}

parse_arguments()
{
    options=$(getopt -o h --long type:,benchmark:,cache:,dwarf,sleep:,config:,help -- "$@")
    eval set -- "$options"
    while :
    do
        case $1 in
            --type)      FLAMEGRAPH_TYPE=$2 ; shift 2 ;;
            --benchmark) BENCHMARK=$2 ; shift 2 ;;
            --cache)     CACHE_TYPE=$2 ; shift 2 ;;
            --dwarf)     DWARF=1 ; shift ;;
            --sleep)     SLEEP=$2 ; shift 2 ;;
            --help)      display_help ; exit ;;
            --) shift ; break ;;
        esac
    done
}

set_cache()
{
    # drop disk cvmfs caches
    if [ $CACHE_TYPE == "cold" ]; then
        sudo rm -r /var/lib/cvmfs/shared 2>/dev/null
    fi

    # check for dropping in memory kernel caches
    if [ $CACHE_TYPE == "cold" ] || [ $CACHE_TYPE == "warm" ]; then
        cvmfs_config umount > /dev/null
    fi
    # we need to mount here, so that perf will know what pid to track
    ls /cvmfs/$REPO > /dev/null
}

trigger_oncpu_profiling()
{
    if [ $DWARF == "1" ]; then
        { perf record -F max -g --call-graph dwarf,64000 -p $PID \
             -o $FLAMEGRAPH_DIR/oncpu_$REPO.perf.data; } &
    else
        { profile -U -f -F 1000 -p $PID > $FLAMEGRAPH_DIR/oncpu_$REPO.out.folded; } &
    fi
    SUBSHELL_PID=$!
    PROFILER_PID=$(ps -ax -o ppid,pid --no-headers | grep ^" "*$SUBSHELL_PID | awk -F' +' '{print $3}');
}

trigger_offcpu_profiling()
{
    if [ $DWARF == "1" ]; then
        { perf record -g --call-graph=dwarf,64000 \
            -e 'sched:sched_switch' -e 'sched:sched_stat_sleep' -e 'sched:sched_stat_blocked' \
            -p $PID -o $FLAMEGRAPH_DIR/offcpu_$REPO.perf.data; } &
    else
        { offcputime -df --stack-storage-size 1000 -p $PID > $FLAMEGRAPH_DIR/offcpu_$REPO.out.folded; } &
    fi
    SUBSHELL_PID=$!
    PROFILER_PID=$(ps -ax -o ppid,pid --no-headers | grep ^" "*$SUBSHELL_PID | awk -F' +' '{print $3}');
}

track_repo()
{
    PID=$(cvmfs_talk -i $REPO pid)
    if [ $FLAMEGRAPH_TYPE == "oncpu" ]; then
        trigger_oncpu_profiling
    else
        trigger_offcpu_profiling
    fi

    sleep 1
    if [[ -n $BENCHMARK ]]; then
        time ./$BENCHMARK #> /dev/null
    else
        sleep $SLEEP
    fi
    # sleep 10
    
    kill -INT $PROFILER_PID
    wait

    if [[ $DWARF == "1" ]]; then
        if [ $FLAMEGRAPH_TYPE == "offcpu" ]; then
            perf script -F time,comm,pid,tid,event,ip,sym,dso,trace -i $FLAMEGRAPH_DIR/offcpu_$REPO.perf.data | \
                stackcollapse-perf-sched.awk -v recurse=1 record_tid=1 | \
                flamegraph.pl --color=io --countname=us >$FLAMEGRAPH_DIR/offcpu_$CACHE_TYPE\_$REPO.svg
        else
            perf script -i $FLAMEGRAPH_DIR/oncpu_$REPO.perf.data | stackcollapse-perf.pl --tid | \
                flamegraph.pl --countname=us >$FLAMEGRAPH_DIR/oncpu_$CACHE_TYPE\_$REPO.svg
        fi
        sudo rm $FLAMEGRAPH_DIR/*$REPO.perf.data
    else
        if [ $FLAMEGRAPH_TYPE == "offcpu" ]; then
            cat $FLAMEGRAPH_DIR/offcpu_$REPO.out.folded | \
                flamegraph.pl --color=io --countname=us >$FLAMEGRAPH_DIR/offcpu_$CACHE_TYPE\_$REPO.svg
        else
            cat $FLAMEGRAPH_DIR/oncpu_$REPO.out.folded | \
                flamegraph.pl >$FLAMEGRAPH_DIR/oncpu_$CACHE_TYPE\_$REPO.svg
        fi
        sudo rm $FLAMEGRAPH_DIR/*$REPO.out.folded
    fi
}

# Run script
DWARF=0
CACHE_TYPE=hot  
SLEEP=10
FLAMEGRAPH_TYPE=oncpu
FLAMEGRAPH_DIR=flamegraphs
REPO=$1
echo $REPO

parse_arguments $@
echo 1 > /proc/sys/kernel/sched_schedstats
mkdir -p $FLAMEGRAPH_DIR
if [[ -z $BENCHMARK ]]; then
    echo "Global profiling for $SLEEP seconds"
    CACHE_TYPE=global
else
    echo "Profiling $BENCHMARK"
    set_cache
fi
track_repo
echo 0 > /proc/sys/kernel/sched_schedstats
