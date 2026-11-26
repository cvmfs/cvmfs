#!/bin/bash
# Round trip: local tree -> cvmfs_server ingestsql-gen -> ingestsql -> mount.
# Files are ingested as external data and served from the S3 bucket's ext/
# prefix, which the mount uses as CVMFS_EXTERNAL_URL.
set -euo pipefail

: "${CVMFS_TEST_REPO:?}" "${CVMFS_TEST_MOUNTPOINT:?}" "${CVMFS_TEST_S3_CONFIG:?}"
: "${CVMFS_TEST_HTTP_BASE:?}" "${CVMFS_S3_BUCKET:?}"

# the lease path is the common prefix; its parent must exist
repo_path=/ingestsqlgen-pkg-1
work=$(mktemp -d)
# ingestsql runs as root and leaves root-owned files behind
trap 'sudo rm -rf "$work"' EXIT
tree=$work/pkg-1

mkdir -p "$tree"/bin "$tree"/lib/sub "$tree"/acl "$tree/with space"
echo hello > "$tree"/bin/app
chmod 0755 "$tree"/bin/app
chmod 0750 "$tree"/bin
: > "$tree"/empty
# 2 full 24 MiB chunks plus a partial one; a chunk-boundary bug breaks the read
head -c 52428800 /dev/urandom > "$tree"/lib/big
head -c 25165824 /dev/urandom > "$tree"/lib/exact
echo x > "$tree/with space/it's"
ln -s bin/app "$tree"/latest
touch "$tree"/lib/sub/.cvmfscatalog
setfacl -m u:1234:r-x "$tree"/acl
touch -d '2026-01-02 03:04:05' "$tree"/bin/app

# the client fetches external data by repository path below CVMFS_EXTERNAL_URL
s3cmd put --recursive "$tree"/ s3://"$CVMFS_S3_BUCKET"/ext"$repo_path"/

cvmfs_server ingestsql-gen -f table -u 0:0 -p "$repo_path" "$tree"
cvmfs_server ingestsql-gen -u 0:0 -p "$repo_path" -o "$work"/pkg-1.db "$tree"

# root: the gateway key is not readable by the test user
sudo -E cvmfs_swissknife ingestsql \
  -W 10 -B "$CVMFS_TEST_MOUNTPOINT" \
  -@ DIRECT \
  -N "$CVMFS_TEST_REPO" \
  -D "$work"/pkg-1.db \
  -w "$CVMFS_TEST_HTTP_BASE/$CVMFS_TEST_REPO" \
  -k /etc/cvmfs/keys/"$CVMFS_TEST_REPO".pub \
  -3 "$CVMFS_TEST_S3_CONFIG" \
  -s /etc/cvmfs/keys/"$CVMFS_TEST_REPO".gw \
  -g http://127.0.0.1:4929/api/v1 \
  -t "$work"

mnt=$CVMFS_TEST_MOUNTPOINT$repo_path
for i in $(seq 30); do
  [ -L "$mnt"/latest ] && break
  sleep 1
done

fail=0
check() {
  if [ "$2" != "$3" ]; then
    echo "MISMATCH $1: expected [$2], got [$3]"
    fail=1
  fi
}

# as root: bin/ is 0750 and owned by 0:0 in the repository
# content, including every chunk of the external files
sudo diff -r --no-dereference -x .cvmfscatalog "$tree" "$mnt" || fail=1
check "symlink" "bin/app" "$(sudo readlink "$mnt"/latest)"
check "bin mode" 750 "$(sudo stat -c %a "$mnt"/bin)"
check "app mode" 755 "$(sudo stat -c %a "$mnt"/bin/app)"
check "app mtime" "$(stat -c %Y "$tree"/bin/app)" \
  "$(sudo stat -c %Y "$mnt"/bin/app)"
check "big size" 52428800 "$(sudo stat -c %s "$mnt"/lib/big)"
check "owner" 0:0 "$(sudo stat -c %u:%g "$mnt"/bin/app)"
check "acl" "$(getfacl -n --omit-header "$tree"/acl)" \
  "$(sudo getfacl -n --omit-header "$mnt"/acl)"
# ingestsql creates the marker for directories with nested=1
sudo test -f "$mnt"/lib/sub/.cvmfscatalog || { echo "lib/sub not nested"; fail=1; }
sudo test ! -e "$mnt"/lib/.cvmfscatalog || { echo "lib is nested"; fail=1; }

[ $fail -eq 0 ] && echo "ingestsql-gen round trip OK"
exit $fail
