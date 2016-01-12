#!/bin/bash

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/../common.sh

SYSTEM_NAME="cc7"
BASE_ARCH="x86_64"
REPO_BASE_URL="http://linuxsoft.cern.ch/cern/centos/7.2/os/$BASE_ARCH/"
GPG_KEY_PATHS="file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7"
BASE_PACKAGES="centos-release coreutils tar iputils rpm yum"

. ${SCRIPT_LOCATION}/../rhel_common/build.sh
