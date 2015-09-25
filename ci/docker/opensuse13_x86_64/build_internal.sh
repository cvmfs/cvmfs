#!/bin/sh

REPO_BASE_URL=http://download.opensuse.org/distribution/13.2/repo/oss/
DESTINATION=/chroot

echo "setting up base system..."
zypper --non-interactive      \
       --gpg-auto-import-keys \
       --root ${DESTINATION}  \
       addrepo $REPO_BASE_URL repo-oss

echo "putting /dev/zero in place..."
mkdir ${DESTINATION}/dev
cp -a /dev/zero ${DESTINATION}/dev/

echo "refreshing metadata cache..."
zypper --non-interactive      \
       --gpg-auto-import-keys \
       --root ${DESTINATION}  \
       refresh

echo "installing package manager..."
zypper --non-interactive     \
       --root ${DESTINATION} \
       install openSUSE-release zypper

echo "cleaning zypper caches..."
zypper --non-interactive     \
       --root ${DESTINATION} \
       clean
