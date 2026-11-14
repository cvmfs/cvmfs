# Testing the CVMFS Azure-MSI publish path without an Azure VM

PR [#4379](https://github.com/cvmfs/cvmfs/pull/4379) adds Azure **Managed
Identity (MSI)** bearer-token auth to the S3/Azure-Blob uploader. The maintainer's
open question was: *can this be tested in CI without a real Azure VM?*

**Yes.** This directory is a runnable proof. It shows that **MinIO cannot** stand
in, but **Azurite `--oauth basic` + a mock IMDS can**, and it reproduces the exact
requests CVMFS emits.

## Why not MinIO

The MSI path speaks two things MinIO does not:

1. **Azure Blob REST dialect** (`x-ms-*` headers, `Put Blob` / `Put Block` /
   `Put Block List`). MinIO implements the AWS S3 API only and
   [explicitly declined](https://github.com/minio/minio/issues/4540) Azure Blob
   REST support. (Its old `gateway azure` mode was the *opposite* direction and was
   [removed in 2022](https://github.com/minio/minio/pull/14418).)
2. **Incoming `Authorization: Bearer` tokens.** MinIO authenticates with S3 SigV4
   access keys (+ STS), not a raw OAuth bearer on a PUT.

So the garage-based `S3 integration tests` already cover the S3 flavor; the Azure
flavor needs an Azure-Blob-speaking endpoint.

## What emulates it

| CVMFS behaviour (PR #4379 / #4395)                                            | Emulated by |
|-------------------------------------------------------------------------------|-------------|
| `RefreshAzureToken()` → GET IMDS token (`api-version=2018-02-01`, `resource=https://storage.azure.com/`, `Metadata: true`) | [`imds_mock.py`](./imds_mock.py) — mints a JWT with the right `aud`/`iss`/`exp`; returns `access_token`+`expires_on` as strings |
| `MkAzureAuthz()` MSI writes: `Authorization: Bearer …`, `x-ms-version: 2017-11-09`, `x-ms-blob-type: BlockBlob` | **Azurite `--oauth basic`** — validates the bearer's `aud`/`iss`/`exp` but **not** its signature ([MS docs](https://learn.microsoft.com/en-us/azure/storage/common/storage-install-azurite)) |
| PR #4395 large blobs: `Put Block` + `Put Block List` (signs `x-ms-blob-content-type`) | Azurite (full block-blob API) |

Azurite's accepted audiences include `https://storage.azure.com/` and its issuer
must be an `https://sts.windows*.net/<tenant>/` prefix — both are baked into the
mock (source: Azurite `src/blob/utils/constants.ts`).

## The one code change required

`RefreshAzureToken()` hard-codes the IMDS endpoint to the link-local
`169.254.169.254`, so nothing off-Azure can answer it.
[`s3fanout-imds-endpoint.patch`](./s3fanout-imds-endpoint.patch) makes it
overridable via `CVMFS_AZURE_IMDS_ENDPOINT` (unset ⇒ identical behaviour on a
real VM). This is the only change needed to make the path CI-testable without
network-level redirection of a link-local IP.

## Run it

```bash
./run-local.sh          # cert + Azurite(--oauth basic) + mock IMDS + verifier, self-cleaning
```

or manually:

```bash
docker compose up -d
python3 verify_msi_path.py
docker compose down -v
```

### Expected output

```
1. RefreshAzureToken() -> mock IMDS
  [PASS] fetched bearer token from IMDS  (807 bytes, 3 JWT segments)
2. Create container (Bearer auth over Azure Blob dialect)
  [PASS] PUT ?restype=container -> HTTP 201  (created)
3. Single-PUT block blob (MkAzureAuthz MSI headers)
  [PASS] PUT blob (x-ms-blob-type: BlockBlob) -> HTTP 201
  [PASS] GET read-back -> HTTP 200  (bytes match)
4. Large blob via Put Block / Put Block List (PR #4395 path)
  [PASS] uploaded 6 blocks via Put Block (comp=block)
  [PASS] Put Block List (comp=blocklist) commit -> HTTP 201
  [PASS] GET large blob read-back -> HTTP 200 (5255225 bytes)  (bytes match)

PASS: Azurite + mock IMDS accepted every CVMFS-shaped MSI request.
```

## Files

| File | Purpose |
|------|---------|
| [`imds_mock.py`](./imds_mock.py) | Zero-dependency fake IMDS token endpoint |
| [`verify_msi_path.py`](./verify_msi_path.py) | Zero-dependency verifier replaying CVMFS's exact request shape |
| [`run-local.sh`](./run-local.sh) | One-command local proof (self-tearing-down) |
| [`docker-compose.yml`](./docker-compose.yml) | Azurite + mock IMDS for CI |
| [`github-workflow-azure-msi.yml`](./github-workflow-azure-msi.yml) | CI: `emulator-smoke` (always runs) + `publisher-e2e` (real publish) |
| [`publish-e2e.sh`](./publish-e2e.sh) | Real `cvmfs_server` mkfs + publish against the emulator over MSI |
| [`s3.conf.template`](./s3.conf.template) | CVMFS S3/azure config for the emulator (MSI, no secret key) |
| [`s3fanout-imds-endpoint.patch`](./s3fanout-imds-endpoint.patch) | Make the IMDS URL overridable (the enabling change) |

## Scope / honesty

- `verify_msi_path.py` proves **the emulator accepts exactly the requests CVMFS's
  MSI code produces** — token fetch, bearer-authenticated Azure-Blob writes, and
  the `Put Block`/`Put Block List` staging from #4395 — without building CVMFS.
  This has been run and passes (see output above).
- The `publisher-e2e` job builds `cvmfs-server` from the PR branch with the patch
  applied and runs a real `cvmfs_server` mkfs + publish ([`publish-e2e.sh`](./publish-e2e.sh))
  against Azurite over MSI, asserting the `.cvmfspublished` manifest lands in the
  blob store. The CVMFS compile runs in CI (or fold it into the existing S3
  integration harness, which already builds cvmfs and runs a containerized publisher).
- `--oauth basic` deliberately skips signature validation — appropriate for a test
  double, not a security check.
