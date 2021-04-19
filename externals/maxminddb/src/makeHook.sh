#!/bin/sh

set -e
rm -rf build

PYTHON=python3
if ! $PYTHON -V >/dev/null 2>&1; then
  PYTHON=python
  if ! $PYTHON -V >/dev/null 2>&1; then
    PYTHON=python2
  fi
fi

$PYTHON setup.py build
if [ -d build/lib ]; then
  cd build/lib
else
  cd build/lib.*
fi
find *|cpio -pdv $EXTERNALS_INSTALL_LOCATION
