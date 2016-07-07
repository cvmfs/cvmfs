#!/bin/bash

#
# This script builds the doxygen documentation
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/common.sh

DOC_OUTPUT=
if [ "x$1" != "x" ]; then
  DOC_OUTPUT="$1"
else
  die "output directory missing"
fi

REPO_ROOT="$(get_repository_root)"

[ -d $REPO_ROOT ] || die "$REPO_ROOT is malformed"

cd $DOC_OUTPUT
cmake -DBUILD_DOCUMENTATION=yes $REPO_ROOT
make doc
