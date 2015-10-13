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

echo "checking for expected public key..."
expected_pubkey1="gpg-pubkey-3dbdc284-53674dd4"
expected_pubkey2="gpg-pubkey-307e3d54-4be01a65"
[ $(rpm -qa | grep gpg-pubkey | wc -l) -eq 2 ] || { echo "more than two keys found"; exit 1; }
rpm -qa | grep $expected_pubkey1               || { echo "public key doesn't match"; exit 1; }
rpm -qa | grep $expected_pubkey2               || { echo "public key doesn't match"; exit 1; }

echo "cleaning zypper caches..."
zypper --non-interactive     \
       --root ${DESTINATION} \
       clean
