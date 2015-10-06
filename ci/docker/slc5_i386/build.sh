#!/bin/bash

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/../common.sh

SYSTEM_NAME="slc5"
BASE_ARCH="i386"
REPO_BASE_URL="http://linuxsoft.cern.ch/cern/slc5X/$BASE_ARCH/yum/os/"
GPG_KEY_PATHS="file:///etc/pki/rpm-gpg/RPM-GPG-KEY-cern"
BASE_PACKAGES="sl-release coreutils tar iputils rpm yum yum-conf"

. ${SCRIPT_LOCATION}/../rhel_common/build.sh
