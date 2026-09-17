#!/usr/bin/env python3
"""
Fake Azure Instance Metadata Service (IMDS) token endpoint.

Emulates the single call CVMFS's S3FanoutManager::RefreshAzureToken() makes:

    GET http://<imds>/metadata/identity/oauth2/token
        ?api-version=2018-02-01&resource=https%3A%2F%2Fstorage.azure.com%2F
    Header: Metadata: true

It returns a JSON body whose `access_token` is a JWT shaped exactly the way
Azurite's `--oauth basic` validator expects (correct `aud` + `iss` prefix +
future `exp`; the signature is NOT verified in basic mode, so we sign with a
throwaway HMAC key). `access_token` and `expires_on` are returned as STRINGS,
because CVMFS reads `expires_on` as a JSON string and feeds it to String2Uint64.

Zero third-party dependencies (stdlib only) so it drops straight into CI.
"""
import base64
import hashlib
import hmac
import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

# Values are cosmetic (basic mode ignores the signature and these claim values);
# they only need to be structurally plausible.
TENANT = os.environ.get("MOCK_TENANT_ID", "72f988bf-86f1-41af-91ab-2d7cd011db47")
APP_ID = os.environ.get("MOCK_APP_ID", "11111111-2222-3333-4444-555555555555")
OID = os.environ.get("MOCK_OBJECT_ID", "99999999-8888-7777-6666-555555555555")
AUDIENCE = os.environ.get("MOCK_AUDIENCE", "https://storage.azure.com/")
TOKEN_TTL = int(os.environ.get("MOCK_TOKEN_TTL", "3600"))


def _b64url(raw: bytes) -> bytes:
    return base64.urlsafe_b64encode(raw).rstrip(b"=")


def mint_jwt() -> str:
    """Mint a JWT that Azurite `--oauth basic` accepts (signature unchecked)."""
    now = int(time.time())
    header = {"alg": "HS256", "typ": "JWT", "kid": "mock-key"}
    payload = {
        "aud": AUDIENCE,
        "iss": f"https://sts.windows.net/{TENANT}/",
        "iat": now,
        "nbf": now,
        "exp": now + TOKEN_TTL,
        "appid": APP_ID,
        "tid": TENANT,
        "oid": OID,
        "sub": OID,
        "ver": "1.0",
        "xms_mirid": (
            f"/subscriptions/{TENANT}/resourcegroups/mock/providers/"
            "Microsoft.ManagedIdentity/userAssignedIdentities/mock"
        ),
    }
    signing_input = _b64url(json.dumps(header).encode()) + b"." + \
        _b64url(json.dumps(payload).encode())
    sig = _b64url(hmac.new(b"mock-signing-key", signing_input,
                           hashlib.sha256).digest())
    return (signing_input + b"." + sig).decode()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # keep CI logs quiet-ish
        print("[imds] " + (fmt % args))

    def do_GET(self):
        parsed = urlparse(self.path)
        if not parsed.path.endswith("/oauth2/token"):
            self.send_error(404, "not the token endpoint")
            return
        # Real IMDS mandates this header; enforce it so the mock is faithful.
        if self.headers.get("Metadata", "").lower() != "true":
            self.send_error(400, "Metadata: true header required")
            return
        qs = parse_qs(parsed.query)
        now = int(time.time())
        body = {
            "access_token": mint_jwt(),
            "client_id": APP_ID,
            "expires_in": str(TOKEN_TTL),
            "expires_on": str(now + TOKEN_TTL),      # STRING (CVMFS parses str)
            "ext_expires_in": str(TOKEN_TTL),
            "not_before": str(now),
            "resource": qs.get("resource", [AUDIENCE])[0],
            "token_type": "Bearer",
        }
        payload = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def main():
    host = os.environ.get("IMDS_BIND", "127.0.0.1")
    port = int(os.environ.get("IMDS_PORT", "8081"))
    srv = ThreadingHTTPServer((host, port), Handler)
    print(f"[imds] mock IMDS listening on http://{host}:{port}"
          f"/metadata/identity/oauth2/token  (aud={AUDIENCE})")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
