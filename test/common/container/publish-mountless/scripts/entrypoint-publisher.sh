#!/bin/bash
# entrypoint-publisher.sh – start-up script for the cvmfs publisher container.
#
# This container connects to the gateway as a mountless publisher
# (connect-gw -P) and then sleeps, waiting for ingest commands.
#
# On first boot it:
#   1. Waits for the gateway API to accept connections.
#   2. Writes the gateway key file for authentication.
#   3. Fetches the repository keys from the gateway.
#   4. Runs `cvmfs_server connect-gw -P` to register as a mountless publisher.
# On every subsequent boot it skips straight to sleeping.
#
# Publishing is done via `cvmfs_server ingest` from this container:
#   docker compose exec publisher cvmfs_server ingest \
#       --tar_file /path/to/content.tar --base_dir / test.repo.org

set -e

# ---------------------------------------------------------------------------
# Configurable via environment variables (docker-compose environment)
# ---------------------------------------------------------------------------
REPO_NAME="${REPO_NAME:-test.repo.org}"
REPO_OWNER="${REPO_OWNER:-root}"
GW_HOST="${GW_HOST:-cvmfs-gateway}"
GW_PORT="${GW_PORT:-4929}"
# Gateway lease key (must match the gateway's key)
GW_KEY_ID="${GW_KEY_ID:-mykey}"
GW_KEY_SECRET="${GW_KEY_SECRET:-mysecret}"
# URL advertised to CVMFS clients
STRATUM0_URL="${STRATUM0_URL:-http://cvmfs.web.garage.internal:3902}"

KEYS_DIR="/etc/cvmfs/keys"
GW_KEY_FILE="${KEYS_DIR}/${REPO_NAME}.gw"
SETUP_DONE_MARKER="/var/spool/cvmfs/.publisher_setup_done"

# ---------------------------------------------------------------------------
# 1. Wait for the gateway API to be reachable
# ---------------------------------------------------------------------------
echo "[entrypoint-publisher] Waiting for gateway API at ${GW_HOST}:${GW_PORT} ..."
until curl -sf --max-time 3 "http://${GW_HOST}:${GW_PORT}/api/v1" > /dev/null 2>&1; do
    sleep 2
done
echo "[entrypoint-publisher] Gateway API is up."

# ---------------------------------------------------------------------------
# 2. First-boot setup
# ---------------------------------------------------------------------------
if [ ! -f "${SETUP_DONE_MARKER}" ]; then
    echo "[entrypoint-publisher] First boot – connecting to gateway for ${REPO_NAME}."

    # Write gateway lease key (must match the key on the gateway side)
    mkdir -p "${KEYS_DIR}"
    echo "plain_text ${GW_KEY_ID} ${GW_KEY_SECRET}" > "${GW_KEY_FILE}"
    echo "[entrypoint-publisher] Gateway key written to ${GW_KEY_FILE}."

    # Connect to the gateway as a mountless publisher.
    # -P  mountless publisher: skip FUSE mount (for use with mountless ingest)
    # -u  gateway URL
    # -w  stratum0 URL for clients
    # -K  fetch .pub and .crt keys from the gateway
    # -o  owning user
    echo "[entrypoint-publisher] Running cvmfs_server connect-gw -P ..."
    cvmfs_server connect-gw \
        -P \
        -K \
        -u "http://${GW_HOST}:${GW_PORT}/api/v1" \
        -w "${STRATUM0_URL}/${REPO_NAME}" \
        -o "${REPO_OWNER}" \
        "${REPO_NAME}"
    echo "[entrypoint-publisher] connect-gw -P complete."

    touch "${SETUP_DONE_MARKER}"
fi

# ---------------------------------------------------------------------------
# Keep the container running so users can exec ingest commands into it.
# ---------------------------------------------------------------------------
echo "[entrypoint-publisher] Publisher ready. Use 'docker compose exec publisher' to run ingest commands."
echo "[entrypoint-publisher] Example:"
echo "  docker compose exec publisher cvmfs_server ingest --tar_file /data/content.tar --base_dir / ${REPO_NAME}"
exec sleep infinity
