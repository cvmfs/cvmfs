#!/bin/bash

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/../common.sh

SYSTEM_NAME="fedora22"
BASE_ARCH="i386"

yum_repo="$(get_yum_repo_mirror "https://mirrors.fedoraproject.org/mirrorlist?repo=fedora-22&arch=$BASE_ARCH")"
REPO_BASE_URL="$yum_repo"
GPG_KEY_PATHS="file:///etc/pki/rpm-gpg/RPM-GPG-KEY-fedora-22"
BASE_PACKAGES="fedora-release coreutils tar iputils dnf"
PACKAGE_MGR="dnf"

. ${SCRIPT_LOCATION}/../rhel_common/build.sh
