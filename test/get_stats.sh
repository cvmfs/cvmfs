#!/usr/bin/bash

#set -x
set -e

. bench_common.sh

OUTFILE=stats.csv

function headers() {
  echo -n "$TEST/$KCACHE/$CACHE/$BACKEND," >> "$OUTFILE"
}

function collect() {
  EXECTIME=$(grep execution "DATA/$RUN/$TEST/$KCACHE/$CACHE/$BACKEND/test.log" | cut -d ' ' -f 3)
  echo -n "$EXECTIME," >> "$OUTFILE"
}

if [ -e "$OUTFILE" ]; then
  die "$OUTFILE" exists
fi

runall headers
echo >> "$OUTFILE"

for RUN in $(/usr/bin/ls DATA/); do
  runall collect
  echo >> "$OUTFILE"
done

sed -i -e 's/,$//' "$OUTFILE"
