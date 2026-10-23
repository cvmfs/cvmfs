#!/bin/sh
# setup_garage.sh – one-shot initialisation of the Garage S3 service.
#
# This script is intended to run as a short-lived init container (or via
# `docker compose exec`) AFTER the Garage daemon has started.  It:
#   1. Waits for the Garage admin API to become available.
#   2. Assigns the single node to a zone and applies the layout.
#   3. Creates an S3 access key with a pre-known ID/secret so the gateway
#      container can be configured with static credentials.
#   4. Creates the CVMFS bucket and grants the key read+write access.
#   5. Enables anonymous (public) read on the bucket so CVMFS clients can
#      download repository data without credentials.
#
# Environment variables (all have sane defaults for local testing):
#   GARAGE_ADMIN_URL    Admin API base URL         (default: http://cvmfs-garage:3903)
#   GARAGE_ADMIN_TOKEN  Admin bearer token         (default: garage-admin-token, matches garage.toml)
#   S3_ACCESS_KEY       Desired S3 access key ID   (default: GKcvmfsaccesskey00000001)
#   S3_SECRET_KEY       Desired S3 secret key      (default: cvmfs_secret_key_placeholder_32chars)
#   S3_BUCKET           Bucket name                (default: cvmfs)

set -e

GARAGE_ADMIN_URL="${GARAGE_ADMIN_URL:-http://cvmfs-garage:3903}"
GARAGE_ADMIN_TOKEN="${GARAGE_ADMIN_TOKEN:-garage-admin-token}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-GKcvmfsaccesskey00000001}"
S3_SECRET_KEY="${S3_SECRET_KEY:-cvmfs_secret_key_placeholder_32chars}"
S3_BUCKET="${S3_BUCKET:-cvmfs}"

AUTH_HEADER="Authorization: Bearer ${GARAGE_ADMIN_TOKEN}"

# ---------------------------------------------------------------------------
# 1. Wait for the admin API
# ---------------------------------------------------------------------------
echo "[setup_garage] Waiting for Garage admin API at ${GARAGE_ADMIN_URL} ..."
until curl -sf -H "${AUTH_HEADER}" "${GARAGE_ADMIN_URL}/v1/health" > /dev/null 2>&1; do
    sleep 2
done
echo "[setup_garage] Garage admin API is ready."

# ---------------------------------------------------------------------------
# 2. Assign the single cluster node to a zone and apply layout
# ---------------------------------------------------------------------------
# Fetch the node ID of the running instance
NODE_ID=$(curl -sf -H "${AUTH_HEADER}" "${GARAGE_ADMIN_URL}/v1/status" \
    | sed 's/.*"node":"\([^"]*\)".*/\1/')

echo "[setup_garage] Node ID: ${NODE_ID}"

# Assign the node: zone=dc1, capacity=1 (arbitrary unit, required for layout)
curl -sf -X POST \
    -H "${AUTH_HEADER}" \
    -H "Content-Type: application/json" \
    -d "[{\"id\":\"${NODE_ID}\",\"zone\":\"dc1\",\"capacity\":1}]" \
    "${GARAGE_ADMIN_URL}/v1/layout" > /dev/null

# Apply the layout (version must increment; start at 1 for a fresh cluster)
curl -sf -X POST \
    -H "${AUTH_HEADER}" \
    -H "Content-Type: application/json" \
    -d '{"version":1}' \
    "${GARAGE_ADMIN_URL}/v1/layout/apply" > /dev/null

echo "[setup_garage] Layout applied."

# ---------------------------------------------------------------------------
# 3. Create the S3 access key with a pre-known ID and secret
#    Using importCredentials lets the gateway container use static env vars.
# ---------------------------------------------------------------------------
curl -sf -X POST \
    -H "${AUTH_HEADER}" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"cvmfs-key\",\"importCredentials\":{\"accessKeyId\":\"${S3_ACCESS_KEY}\",\"secretAccessKey\":\"${S3_SECRET_KEY}\"}}" \
    "${GARAGE_ADMIN_URL}/v1/key" > /dev/null

echo "[setup_garage] S3 key created (id=${S3_ACCESS_KEY})."

# ---------------------------------------------------------------------------
# 4. Create the bucket and grant the key read+write access
# ---------------------------------------------------------------------------
BUCKET_ID=$(curl -sf -X POST \
    -H "${AUTH_HEADER}" \
    -H "Content-Type: application/json" \
    -d "{\"globalAlias\":\"${S3_BUCKET}\"}" \
    "${GARAGE_ADMIN_URL}/v1/bucket" \
    | sed 's/.*"id":"\([^"]*\)".*/\1/')

echo "[setup_garage] Bucket '${S3_BUCKET}' created (id=${BUCKET_ID})."

curl -sf -X POST \
    -H "${AUTH_HEADER}" \
    -H "Content-Type: application/json" \
    -d "{\"bucketId\":\"${BUCKET_ID}\",\"accessKeyId\":\"${S3_ACCESS_KEY}\",\"permissions\":{\"read\":true,\"write\":true,\"owner\":false}}" \
    "${GARAGE_ADMIN_URL}/v1/bucket/allow" > /dev/null

echo "[setup_garage] Key granted read+write on bucket '${S3_BUCKET}'."

# ---------------------------------------------------------------------------
# 5. Allow anonymous (public) read so CVMFS clients can download without creds
# ---------------------------------------------------------------------------
curl -sf -X POST \
    -H "${AUTH_HEADER}" \
    -H "Content-Type: application/json" \
    -d "{\"bucketId\":\"${BUCKET_ID}\",\"accessKeyId\":null,\"permissions\":{\"read\":true,\"write\":false,\"owner\":false}}" \
    "${GARAGE_ADMIN_URL}/v1/bucket/allow" > /dev/null

echo "[setup_garage] Anonymous read enabled on bucket '${S3_BUCKET}'."
echo "[setup_garage] Garage setup complete."

