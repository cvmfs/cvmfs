#!/bin/sh

set -e

if [ ! -f CMakeLists.txt ]; then
  git init
  git remote add origin https://github.com/cooperative-computing-lab/cctools.git
  git fetch
  git checkout 0317c6a974dd760388f6da83017d98bedc157754
  ./configure --prefix "$(pwd)/build"
fi
