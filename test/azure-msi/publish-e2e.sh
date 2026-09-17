#!/usr/bin/env bash
# Real end-to-end publish test: a patched cvmfs_server publishes to the Azurite
# Azure-Blob emulator over MSI (no Azure VM, no storage key). Run AFTER building
# cvmfs-server from PR #4379 with s3fanout-imds-endpoint.patch applied, with the
# emulator (docker compose up -d) already running.
#
# Asserts: mkfs succeeds, a transaction publishes, and the resulting
# .cvmfspublished manifest is retrievable from the blob store (bearer read).
set -euo pipefail
D="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="${REPO_FQRN:-test.eessi.local}"
BLOB="https://127.0.0.1:10000/devstoreaccount1/cvmfs"

# The two knobs the patch + emulator require:
export CVMFS_AZURE_IMDS_ENDPOINT="${CVMFS_AZURE_IMDS_ENDPOINT:-http://127.0.0.1:8081}"
export CURL_CA_BUNDLE="${CURL_CA_BUNDLE:-$D/certs/azurite-cert.pem}"   # trust Azurite's self-signed TLS

echo "== 1. mkfs (S3/azure backend, MSI auth) =="
sudo -E cvmfs_server mkfs -w "$BLOB" -s "$D/s3.conf.template" -o "$(id -un)" "$REPO"

echo "== 2. transaction + publish =="
sudo -E cvmfs_server transaction "$REPO"
echo "hello from the MSI CI e2e $(date -u)" | sudo tee "/cvmfs/$REPO/hello.txt" >/dev/null
sudo -E cvmfs_server publish "$REPO"

echo "== 3. assert manifest landed in blob store (bearer read) =="
# Grab a token from the mock IMDS exactly as cvmfs does, then GET .cvmfspublished.
TOKEN="$(curl -s -H 'Metadata: true' \
  "$CVMFS_AZURE_IMDS_ENDPOINT/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https%3A%2F%2Fstorage.azure.com%2F" \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"])')"
code="$(curl -s -o /tmp/cvmfspublished -w '%{http_code}' \
  -H "Authorization: Bearer $TOKEN" -H "x-ms-version: 2017-11-09" \
  "$BLOB/.cvmfspublished")"
echo "GET .cvmfspublished -> HTTP $code"
test "$code" = "200"
grep -q '^C' /tmp/cvmfspublished && echo "manifest has a root-catalog hash line (C...)"

echo "== 4. server-side sanity =="
sudo -E cvmfs_server check "$REPO"
echo "PASS: patched cvmfs_server published to the Azure-Blob emulator over MSI."
