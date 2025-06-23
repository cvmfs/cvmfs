#!/bin/bash

display_help()
{
    echo -e "Generate average time statistics for a given benchmark, for all the CVMFS caching scenarios."
    echo -e "Optionally, a local (non-cvmfs) version of the benchmark can be included in the statistics.\n"
    echo -e \
		 "Usage: ./avg_cache_time.sh <benchmark name> <number of rounds to run the benchmark> [local benchmark name]"
}

if test $# -lt 2; then
    display_help;
    exit 1
fi

if ! [[ $2 =~ [1-9][0-9]* ]]; then
    echo "Invalid number of running rounds"
    exit 1
fi

BENCHMARK=$1
ROUNDS=$2
if test $# -gt 2; then
    LOCAL_BENCHMARK=$3
fi
CACHE_TIME=helpers/cache_time.sh
AVG_TIME=helpers/avg_cache_time.py

TMP_DIR=$(mktemp -d)

for i in $(seq 1 $ROUNDS)
do
    $CACHE_TIME cold $BENCHMARK 2>> $TMP_DIR/cold_results
    $CACHE_TIME warm $BENCHMARK 2>> $TMP_DIR/warm_results
    $CACHE_TIME hot $BENCHMARK 2>> $TMP_DIR/hot_results
    if test $# -gt 2;then
        $CACHE_TIME no_cache $LOCAL_BENCHMARK 2>> $TMP_DIR/local_results
    fi
done

sed -i '/^$/d' $TMP_DIR/*results

python3 $AVG_TIME $ROUNDS $TMP_DIR 
