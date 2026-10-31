#!/usr/bin/env bash
# Local end-to-end proof: Azurite (--oauth basic) + mock IMDS accept the exact
# request shapes CVMFS's Azure-MSI uploader emits. One foreground command; tears
# itself down on exit. Requires: docker, python3, openssl, curl.
set -euo pipefail
D="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$D"
NAME="${AZURITE_CONTAINER_NAME:-azurite-msi}"
IMDS_PID=""

cleanup() {
  [[ -n "$IMDS_PID" ]] && kill "$IMDS_PID" >/dev/null 2>&1 || true
  docker rm -f "$NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# 1. self-signed cert for Azurite HTTPS (--oauth basic requires TLS)
if [[ ! -f certs/azurite-cert.pem ]]; then
  mkdir -p certs
  openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout certs/azurite-key.pem -out certs/azurite-cert.pem -days 3650 \
    -subj "/CN=127.0.0.1" \
    -addext "subjectAltName=IP:127.0.0.1,DNS:localhost" 2>/dev/null
fi

# 2. Azurite blob service, OAuth basic (validates aud/iss/exp, NOT signature)
docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" -p 10000:10000 -v "$D/certs:/certs:ro" \
  mcr.microsoft.com/azure-storage/azurite \
  azurite-blob --blobHost 0.0.0.0 --blobPort 10000 --oauth basic \
  --cert /certs/azurite-cert.pem --key /certs/azurite-key.pem \
  --skipApiVersionCheck >/dev/null
echo "[run] azurite started ($NAME)"

# 3. mock IMDS (stdout to file so it never holds the parent's pipe)
python3 imds_mock.py >/tmp/imds_mock.log 2>&1 &
IMDS_PID=$!
echo "[run] mock IMDS started (pid $IMDS_PID)"

# 4. wait for readiness
for _ in $(seq 1 30); do
  [[ "$(curl -sk -o /dev/null -w '%{http_code}' \
        "https://127.0.0.1:10000/devstoreaccount1?comp=list" || true)" != "000" ]] && break
  sleep 1
done
for _ in $(seq 1 15); do
  curl -sf -o /dev/null -H "Metadata: true" \
    "http://127.0.0.1:8081/metadata/identity/oauth2/token?resource=x" && break || sleep 1
done
echo "[run] services ready; running verifier"
echo "------------------------------------------------------------"

# 5. the actual proof
rc=0
python3 verify_msi_path.py || rc=$?
echo "------------------------------------------------------------"
echo "[run] verifier exit code: $rc"
exit $rc
