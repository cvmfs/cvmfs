#!/bin/sh
# setup_garage.sh – one-shot initialisation of the Garage S3 service (v2 API).
#
# This script is intended to run as a short-lived init container (or via
# `docker compose exec`) AFTER the Garage daemon has started.  It:
#   1. Waits for the Garage admin API to become available.
#   2. Assigns the single node to a zone and applies the layout.
#   3. Imports an S3 access key with a pre-known ID/secret so the gateway
#      container can be configured with static credentials.
#   4. Creates the CVMFS bucket and grants the key read+write access.
#   5. Enables website access on the bucket so CVMFS clients can download
#      repository data without S3 credentials (via the Garage web endpoint).
#
# Environment variables (all have sane defaults for local testing):
#   GARAGE_ADMIN_URL    Admin API base URL         (default: http://cvmfs-garage:3903)
#   GARAGE_ADMIN_TOKEN  Admin bearer token         (default: garage-admin-token, matches garage.toml)
#   S3_ACCESS_KEY       Desired S3 access key ID   (default: GKcvmfsaccesskey00000001)
#   S3_SECRET_KEY       Desired S3 secret key      (default: cvmfs_secret_key_placeholder_32chars)
#   S3_BUCKET           Bucket name                (default: cvmfs)

set -eu

# Enable tracing in CI for easier debugging
if [ "${CI:-}" = "true" ] || [ "${SETUP_GARAGE_DEBUG:-}" = "1" ]; then
  set -x
fi

GARAGE_ADMIN_URL="${GARAGE_ADMIN_URL:-http://cvmfs-garage:3903}"
GARAGE_ADMIN_TOKEN="${GARAGE_ADMIN_TOKEN:-garage-admin-token}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-GKcvmfsaccesskey00000001}"
S3_SECRET_KEY="${S3_SECRET_KEY:-cvmfs_secret_key_placeholder_32chars}"
S3_BUCKET="${S3_BUCKET:-cvmfs}"

AUTH_HEADER="Authorization: Bearer ${GARAGE_ADMIN_TOKEN}"

# Helper: make an API call and log the result for debugging.
# Usage: api_call <method> <endpoint> [json_body]
#
# Uses curl WITHOUT -f so we always capture the response body (even on
# HTTP errors).  The HTTP status code is checked explicitly.
api_call() {
    _method="$1"
    _endpoint="$2"
    _body="${3:-}"

    _url="${GARAGE_ADMIN_URL}${_endpoint}"
    echo "[setup_garage]   -> ${_method} ${_endpoint}"

    _tmpfile=$(mktemp)
    if [ -n "${_body}" ]; then
        echo "[setup_garage]      body: ${_body}"
        _http_code=$(curl -sS -o "${_tmpfile}" -w '%{http_code}' \
            -X "${_method}" \
            -H "${AUTH_HEADER}" \
            -H "Content-Type: application/json" \
            -d "${_body}" \
            "${_url}") || {
                _rc=$?
                echo "[setup_garage]   !! curl failed (exit ${_rc}): $(cat "${_tmpfile}" 2>/dev/null)"
                rm -f "${_tmpfile}"
                return 1
            }
    else
        _http_code=$(curl -sS -o "${_tmpfile}" -w '%{http_code}' \
            -X "${_method}" \
            -H "${AUTH_HEADER}" \
            "${_url}") || {
                _rc=$?
                echo "[setup_garage]   !! curl failed (exit ${_rc}): $(cat "${_tmpfile}" 2>/dev/null)"
                rm -f "${_tmpfile}"
                return 1
            }
    fi

    _resp=$(cat "${_tmpfile}")
    rm -f "${_tmpfile}"

    echo "[setup_garage]      HTTP ${_http_code}: ${_resp}"

    case "${_http_code}" in
        2[0-9][0-9])
            ;; # 2xx – success
        *)
            echo "[setup_garage]   !! API error HTTP ${_http_code}: ${_resp}"
            return 1
            ;;
    esac

    # Store response for callers to parse
    API_RESP="${_resp}"
}

# ---------------------------------------------------------------------------
# 1. Wait for the admin API
# ---------------------------------------------------------------------------
# Wait for the admin API to accept connections.  We use /v2/GetClusterStatus
# (with auth) rather than /health because /health returns 503 before a layout
# is applied (no storage nodes → no quorum), creating a deadlock.
echo "[setup_garage] Waiting for Garage admin API at ${GARAGE_ADMIN_URL} ..."
_attempts=0
_max_attempts=30
until curl -sS -o /dev/null -w '' -H "${AUTH_HEADER}" "${GARAGE_ADMIN_URL}/v2/GetClusterStatus" 2>/dev/null; do
    _attempts=$((_attempts + 1))
    if [ "${_attempts}" -ge "${_max_attempts}" ]; then
        echo "[setup_garage] ERROR: Garage admin API not ready after $((_max_attempts * 2))s"
        echo "[setup_garage] Last curl attempt:"
        curl -sS -v -H "${AUTH_HEADER}" "${GARAGE_ADMIN_URL}/v2/GetClusterStatus" 2>&1 || true
        exit 1
    fi
    sleep 2
done
echo "[setup_garage] Garage admin API is ready (after $((_attempts * 2))s)."

# ---------------------------------------------------------------------------
# 2. Assign the single cluster node to a zone and apply layout
# ---------------------------------------------------------------------------
# Fetch the cluster status (v2 API)
echo "[setup_garage] Fetching cluster status ..."
api_call GET "/v2/GetClusterStatus"

# Extract the first node ID from the response.
# Response format: {"layoutVersion":0,"nodes":[{"id":"<hex>", ...}]}
# Use jq if available, fall back to sed.
if command -v jq >/dev/null 2>&1; then
    NODE_ID=$(echo "${API_RESP}" | jq -r '.nodes[0].id')
else
    NODE_ID=$(echo "${API_RESP}" | sed 's/.*"id":"\([^"]*\)".*/\1/' | head -c 64)
fi
echo "[setup_garage] Node ID: ${NODE_ID}"

if [ -z "${NODE_ID}" ] || [ "${#NODE_ID}" -lt 16 ]; then
    echo "[setup_garage] ERROR: could not extract a valid node ID from cluster status."
    echo "[setup_garage] Full response: ${API_RESP}"
    exit 1
fi

# Assign the node: zone=dc1, capacity=1GB (in bytes, required for layout).
# v2 API: POST /v2/UpdateClusterLayout
echo "[setup_garage] Assigning node to layout ..."
api_call POST "/v2/UpdateClusterLayout" \
    "{\"roles\":[{\"id\":\"${NODE_ID}\",\"zone\":\"dc1\",\"capacity\":1000000000,\"tags\":[]}]}"

# Apply the layout (version must increment; start at 1 for a fresh cluster).
# v2 API: POST /v2/ApplyClusterLayout
echo "[setup_garage] Applying layout ..."
api_call POST "/v2/ApplyClusterLayout" '{"version":1}'

echo "[setup_garage] Layout applied."

# ---------------------------------------------------------------------------
# 3. Import the S3 access key with a pre-known ID and secret
#    v2 API: POST /v2/ImportKey
# ---------------------------------------------------------------------------
echo "[setup_garage] Importing S3 key ..."
api_call POST "/v2/ImportKey" \
    "{\"name\":\"cvmfs-key\",\"accessKeyId\":\"${S3_ACCESS_KEY}\",\"secretAccessKey\":\"${S3_SECRET_KEY}\"}"

echo "[setup_garage] S3 key imported (id=${S3_ACCESS_KEY})."

# ---------------------------------------------------------------------------
# 4. Create the bucket and grant the key read+write access
#    v2 API: POST /v2/CreateBucket, POST /v2/AllowBucketKey
# ---------------------------------------------------------------------------
echo "[setup_garage] Creating bucket '${S3_BUCKET}' ..."
api_call POST "/v2/CreateBucket" "{\"globalAlias\":\"${S3_BUCKET}\"}"

# Extract bucket ID from response.
# Response format: {"id":"<hex>", ...}
if command -v jq >/dev/null 2>&1; then
    BUCKET_ID=$(echo "${API_RESP}" | jq -r '.id')
else
    BUCKET_ID=$(echo "${API_RESP}" | sed 's/.*"id":"\([^"]*\)".*/\1/')
fi
echo "[setup_garage] Bucket '${S3_BUCKET}' created (id=${BUCKET_ID})."

if [ -z "${BUCKET_ID}" ]; then
    echo "[setup_garage] ERROR: could not extract bucket ID from CreateBucket response."
    exit 1
fi

echo "[setup_garage] Granting key read+write on bucket ..."
api_call POST "/v2/AllowBucketKey" \
    "{\"bucketId\":\"${BUCKET_ID}\",\"accessKeyId\":\"${S3_ACCESS_KEY}\",\"permissions\":{\"read\":true,\"write\":true,\"owner\":true}}"

echo "[setup_garage] Key granted read+write on bucket '${S3_BUCKET}'."

# ---------------------------------------------------------------------------
# 5. Enable website access on the bucket for anonymous reads
#    In Garage v2, anonymous S3 access is provided through the web endpoint
#    (port 3902).  Enabling website access allows unauthenticated HTTP GETs
#    via the s3_web listener.
#    v2 API: POST /v2/UpdateBucket?id=<bucket_id>
# ---------------------------------------------------------------------------
echo "[setup_garage] Enabling website access on bucket '${S3_BUCKET}' ..."
api_call POST "/v2/UpdateBucket?id=${BUCKET_ID}" \
    '{"websiteAccess":{"enabled":true,"indexDocument":"index.html"}}'

echo "[setup_garage] Website access enabled on bucket '${S3_BUCKET}'."
echo "[setup_garage] Garage setup complete."
