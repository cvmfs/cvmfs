#!/usr/bin/bash

. bench_common.sh

function benchmarks() {
  echo "$RUN/$TEST/$KCACHE/$CACHE/$BACKEND"
  ./benchmark.sh -t "$TEST" -k "$KCACHE" -c "$CACHE" -b "$BACKEND" -o "DATA/$RUN/$TEST/$KCACHE/$CACHE/$BACKEND"
}

#set -x
set -e

sudo -v

for RUN in $(seq 10); do
  runall benchmarks
done

sudo -K

echo 'done!'
