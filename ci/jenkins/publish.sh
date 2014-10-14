#/bin/sh -e

set -e

echo "Continuous Integration Package Publishing Script"

script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common.sh

echo "Coming Soon!"
