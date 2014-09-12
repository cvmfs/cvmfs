#/bin/sh -e

set -e

echo "Continuous Integration Package Test Installation Script"

script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common.sh

case $PACKAGE_TYPE in
  rpm)
    sh ci/jenkins/install_rpm.sh
    ;;
  deb)
    sh ci/jenkins/install_deb.sh
    ;;
  *)
    "FAIL: unknown package type '$PACKAGE_TYPE'"
    ;;
esac
