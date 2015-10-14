#!/bin/bash

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/../common.sh

SYSTEM_NAME="slc4"
BASE_ARCH="i386"
REPO_BASE_URL="http://cvm-storage00.cern.ch/yum/sl4/os/${BASE_ARCH}/RPMS/"
GPG_KEY_PATHS="file:///etc/pki/rpm-gpg/RPM-GPG-KEY-cern  \
               file:///etc/pki/rpm-gpg/RPM-GPG-KEY-csieh \
               file:///etc/pki/rpm-gpg/RPM-GPG-KEY-sl    \
               file:///etc/pki/rpm-gpg/RPM-GPG-KEY-dawson"
BASE_PACKAGES="sl-release coreutils tar iputils rpm yum yum-conf"

. ${SCRIPT_LOCATION}/../rhel_common/build.sh
