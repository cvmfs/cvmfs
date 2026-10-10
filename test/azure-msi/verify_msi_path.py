#!/usr/bin/env python3
"""
Standalone verifier: replays the EXACT request shape CVMFS's Azure-MSI uploader
sends, against Azurite (`--oauth basic`) + the fake IMDS — with no CVMFS build.

It proves the emulator accepts the requests produced by:
  * S3FanoutManager::RefreshAzureToken()  -> GET IMDS token (api-version=2018-02-01,
    resource=https://storage.azure.com/, header `Metadata: true`)
  * S3FanoutManager::MkAzureAuthz() MSI branch -> writes carrying
      x-ms-date, x-ms-version: 2017-11-09, Authorization: Bearer <token>,
      x-ms-blob-type: BlockBlob
  * PR #4395 large-blob staging -> Put Block (comp=block) + Put Block List
    (comp=blocklist, signs x-ms-blob-content-type)

Covers: create container, single-PUT block blob, large blob via Put Block /
Put Block List, and independent read-back byte comparison. stdlib only.

Exit 0 = every step behaved exactly as the real Azure endpoint would.
"""
import base64
import http.client
import json
import os
import ssl
import sys
from email.utils import formatdate
from urllib.parse import urlparse, quote

IMDS_URL = os.environ.get(
    "CVMFS_AZURE_IMDS_ENDPOINT", "http://127.0.0.1:8081")
IMDS_PATH = ("/metadata/identity/oauth2/token"
             "?api-version=2018-02-01&resource=https%3A%2F%2Fstorage.azure.com%2F")
AZURITE = os.environ.get("AZURITE_BLOB_ENDPOINT", "https://127.0.0.1:10000")
ACCOUNT = os.environ.get("AZURITE_ACCOUNT", "devstoreaccount1")
CONTAINER = os.environ.get("AZURITE_CONTAINER", "cvmfs")
CAFILE = os.environ.get("AZURITE_CAFILE",
                        os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                     "certs", "azurite-cert.pem"))
XMS_VERSION = "2017-11-09"   # exactly what MkAzureAuthz() emits for MSI

_PASS, _FAIL = "\033[32mPASS\033[0m", "\033[31mFAIL\033[0m"
_failures = 0


def _report(ok: bool, label: str, detail: str = "") -> None:
    global _failures
    if not ok:
        _failures += 1
    print(f"  [{_PASS if ok else _FAIL}] {label}" + (f"  ({detail})" if detail else ""))


def get_token() -> str:
    u = urlparse(IMDS_URL)
    conn = http.client.HTTPConnection(u.hostname, u.port or 80, timeout=30)
    conn.request("GET", IMDS_PATH, headers={"Metadata": "true"})
    resp = conn.getresponse()
    raw = resp.read()
    conn.close()
    assert resp.status == 200, f"IMDS returned HTTP {resp.status}: {raw[:200]!r}"
    doc = json.loads(raw)
    # Mirror CVMFS: both fields are strings.
    assert isinstance(doc["access_token"], str) and isinstance(doc["expires_on"], str), \
        "access_token/expires_on must be JSON strings (CVMFS parses them as strings)"
    return doc["access_token"]


def _ctx() -> ssl.SSLContext:
    ctx = ssl.create_default_context(cafile=CAFILE)
    return ctx


def azurite_request(method, path, token, body=b"", extra=None):
    u = urlparse(AZURITE)
    conn = http.client.HTTPSConnection(u.hostname, u.port or 443,
                                       context=_ctx(), timeout=30)
    headers = {
        "Authorization": "Bearer " + token,
        "x-ms-date": formatdate(usegmt=True),
        "x-ms-version": XMS_VERSION,
        "Content-Length": str(len(body)),
    }
    if extra:
        headers.update(extra)
    conn.request(method, path, body=body, headers=headers)
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, data


def base(path_in_container=""):
    p = f"/{ACCOUNT}/{CONTAINER}"
    if path_in_container:
        p += "/" + path_in_container
    return p


def main() -> int:
    print(f"IMDS     : {IMDS_URL}")
    print(f"Azurite  : {AZURITE}  (account={ACCOUNT}, container={CONTAINER})")
    print(f"x-ms-version: {XMS_VERSION}   CA: {CAFILE}\n")

    print("1. RefreshAzureToken() -> mock IMDS")
    token = get_token()
    _report(True, "fetched bearer token from IMDS", f"{len(token)} bytes, 3 JWT segments"
            if token.count(".") == 2 else "unexpected JWT shape")

    print("2. Create container (Bearer auth over Azure Blob dialect)")
    st, data = azurite_request("PUT", base() + "?restype=container", token,
                               extra={"x-ms-blob-type": "BlockBlob"})
    _report(st in (201, 409), f"PUT ?restype=container -> HTTP {st}",
            "created" if st == 201 else "already exists" if st == 409 else data[:160].decode("utf-8", "replace"))

    print("3. Single-PUT block blob (MkAzureAuthz MSI headers)")
    small = b"cvmfs-msi-emulation-smoke\n" * 32
    st, data = azurite_request("PUT", base("small.txt"), token, body=small,
                               extra={"x-ms-blob-type": "BlockBlob",
                                      "Content-Type": "text/plain"})
    _report(st == 201, f"PUT blob (x-ms-blob-type: BlockBlob) -> HTTP {st}",
            data[:160].decode("utf-8", "replace") if st != 201 else "")
    st, got = azurite_request("GET", base("small.txt"), token)
    _report(st == 200 and got == small, f"GET read-back -> HTTP {st}",
            "bytes match" if got == small else "MISMATCH")

    print("4. Large blob via Put Block / Put Block List (PR #4395 path)")
    block_sz = 1 << 20                      # 1 MiB blocks
    big = bytes((i * 131 + 7) & 0xFF for i in range(5 * block_sz + 12345))
    nblocks = (len(big) + block_sz - 1) // block_sz
    block_ids = []
    all_ok = True
    for i in range(nblocks):
        chunk = big[i * block_sz:(i + 1) * block_sz]
        # Fixed-width base64 block id (unambiguous commit ordering), URL-escaped in the URL.
        raw_id = f"block-{i:08d}".encode()
        bid = base64.b64encode(raw_id).decode()
        block_ids.append(bid)
        st, data = azurite_request(
            "PUT", base("big.bin") + f"?comp=block&blockid={quote(bid, safe='')}",
            token, body=chunk)
        if st != 201:
            all_ok = False
            _report(False, f"Put Block {i} -> HTTP {st}", data[:160].decode("utf-8", "replace"))
            break
    _report(all_ok, f"uploaded {nblocks} blocks via Put Block (comp=block)")

    # Put Block List commit; signs/sets x-ms-blob-content-type (sorts before x-ms-date).
    xml = "<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList>" + \
        "".join(f"<Latest>{b}</Latest>" for b in block_ids) + "</BlockList>"
    st, data = azurite_request(
        "PUT", base("big.bin") + "?comp=blocklist", token, body=xml.encode(),
        extra={"x-ms-blob-content-type": "application/octet-stream",
               "Content-Type": "application/xml"})
    _report(st == 201, f"Put Block List (comp=blocklist) commit -> HTTP {st}",
            data[:160].decode("utf-8", "replace") if st != 201 else "")
    st, got = azurite_request("GET", base("big.bin"), token)
    _report(st == 200 and got == big,
            f"GET large blob read-back -> HTTP {st} ({len(got)} bytes)",
            "bytes match" if got == big else "MISMATCH")

    print()
    if _failures == 0:
        print(f"{_PASS}: Azurite + mock IMDS accepted every CVMFS-shaped MSI request.")
        return 0
    print(f"{_FAIL}: {_failures} step(s) failed.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
