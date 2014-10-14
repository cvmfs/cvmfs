#!/bin/sh

set -e

echo "Continuous Integration Quick Test Script"

script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common.sh

cvmfs_unittests --gtest_shuffle
