#!/bin/bash

filename=$1

while read -r line;
do
  echo $line
  cat "$line" > /dev/null
done < "$filename"