# Mountless Ingest: Publishing Tarballs Without FUSE

CernVM-FS supports **mountless ingest**, a mode where a publisher pushes
content into a repository without ever mounting a FUSE filesystem or using
an overlay filesystem.  All that is needed is the `cvmfs_server` tooling and
network access to a CernVM-FS gateway.

This is particularly useful for containerised or unprivileged environments
where FUSE is unavailable.

## Overview

In traditional CernVM-FS publishing, `cvmfs_server` opens a transaction that
mounts an overlay on top of the repository's FUSE client mount.  The publisher
modifies files in the overlay and then publishes the resulting changeset.

Mountless ingest replaces this workflow:

1. The **gateway** container runs `cvmfs_server mkfs -P` to create the
   repository without ever mounting FUSE.
2. A separate **publisher** container registers itself with
   `cvmfs_server connect-gw -P` (also mountless).
3. The publisher runs `cvmfs_server ingest` to push tarballs directly into
   the repository through the gateway.

No container in this stack requires privileged mode or systemd.

---

## Quick Start: Publishing a Tarball to an Existing Gateway

If a gateway is already running and the repository exists, all you need is a
machine with `cvmfs-server` installed and connectivity to the gateway API.

### 1. Connect to the gateway (once per publisher)

```bash
# Write the shared gateway key
mkdir -p /etc/cvmfs/keys
echo "plain_text <KEY_ID> <KEY_SECRET>" > /etc/cvmfs/keys/<REPO_NAME>.gw

# Register as a mountless publisher
cvmfs_server connect-gw \
    -P \
    -K \
    -u http://<GATEWAY_HOST>:4929/api/v1 \
    -w <STRATUM0_URL>/<REPO_NAME> \
    -o <REPO_OWNER> \
    <REPO_NAME>
```

Flags:
| Flag | Purpose |
|------|---------|
| `-P` | Mountless publisher mode — skip FUSE mount |
| `-K` | Fetch the repository's `.pub` and `.crt` keys from the gateway |
| `-u` | Gateway API URL |
| `-w` | Stratum-0 URL as seen by clients (used in repository config) |
| `-o` | Repository owner user |

### 2. Ingest a tarball

```bash
cvmfs_server ingest \
    --tar_file /path/to/content.tar \
    -b /cvmfs/<REPO_NAME>/destination/subdir
```

This opens a lease on the gateway, processes the tarball through
`cvmfs_swissknife ingest`, uploads objects to the storage backend, and
closes the lease — all in a single command.

#### Absolute-path shorthand

When `--base_dir` or `--delete` is given as an absolute path under `/cvmfs/`,
the repository name and the subdirectory are extracted automatically.  The
positional `<REPO_NAME>` argument can then be omitted:

```bash
# These two commands are equivalent:
cvmfs_server ingest -t content.tar -b /cvmfs/test.repo.org/software/v2
cvmfs_server ingest -t content.tar -b software/v2 test.repo.org
```

This mirrors the path layout that a mounted CVMFS client would expose and
makes it easy to script ingest commands using the same paths.

### `cvmfs_server ingest` options

| Flag | Long form | Description |
|------|-----------|-------------|
| `-t` | `--tar_file` | Path to the tarball to extract (use `-` for stdin) |
| `-b` | `--base_dir` | Directory inside the repository where the tarball is extracted. Accepts absolute `/cvmfs/<repo>/<path>` or relative `<path>` (with repo as positional arg) |
| `-d` | `--delete` | Path to delete before extraction (one per invocation in gateway mode). Also accepts `/cvmfs/<repo>/<path>` |
| `-c` | `--catalog` | Create a nested catalog at the extraction directory |
| `-u` | `--user` | Owner user of ingested files |
| `-g` | `--group` | Owner group of ingested files |
| `-k` | `--keep-ownership` | Preserve the tarball's original uid/gid |
| `-f` | `--fast-delete` | Use fast deletion for nested catalogs |

**Gateway mode restrictions:**
- Only one `-d` / `--delete` path per invocation (gateway leases are path-scoped).
- Cannot combine `--delete` and `--tar_file` in the same invocation.

### 3. Deleting content

```bash
cvmfs_server ingest -d /cvmfs/<REPO_NAME>/path/to/remove
```

---

## Direct-to-S3 Data Upload (Prototype)

When the publisher has direct S3 credentials, data objects can bypass the
gateway and be written straight to S3.  Only catalog objects (metadata) are
still routed through the gateway to ensure consistency.

This is useful for high-throughput publishing where the gateway would otherwise
become a bottleneck for large data volumes.

### Setup

Place an S3 configuration file at `/etc/cvmfs/<REPO_NAME>.s3.conf`:

```ini
CVMFS_S3_HOST=<S3_HOST>:<S3_PORT>
CVMFS_S3_ACCESS_KEY=<ACCESS_KEY>
CVMFS_S3_SECRET_KEY=<SECRET_KEY>
CVMFS_S3_BUCKET=<BUCKET>
CVMFS_S3_DNS_BUCKETS=false
CVMFS_S3_MAX_NUMBER_OF_PARALLEL_CONNECTIONS=10
CVMFS_S3_FLAVOR=awsv4
CVMFS_S3_REGION=<REGION>
```

When this file exists and the publisher is in mountless gateway mode, the
ingest command automatically detects it and uploads data chunks directly to
S3.  No additional flags are needed — the user runs the same
`cvmfs_server ingest` command:

```bash
cvmfs_server ingest -t /path/to/content.tar -b /cvmfs/<REPO_NAME>/
```

You will see the log line:

```
Info: using direct-to-S3 upload for data objects (/etc/cvmfs/<REPO_NAME>.s3.conf)
```

### How it works

The `GatewayS3Uploader` extends the standard `GatewayUploader`:

- **Data chunks** (content-addressed blobs) are written directly to S3 via
  the `S3FanoutManager`, bypassing the gateway entirely.
- **Catalog objects** (SQLite metadata databases) are posted to the gateway's
  `POST /api/v1/catalogs/:token` endpoint, which ensures transactional
  consistency of the repository's Merkle tree.

---

## Containerised Test Setup

The directory `test/common/container/publish-mountless/` provides a complete
Docker Compose stack for testing mountless ingest.  It runs entirely without
privileged mode.

### Architecture

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│   garage     │◄────│   gateway    │     │  publisher   │
│  (S3 store)  │     │  mkfs -P -D  │◄────│ connect-gw -P│
│  port 3900   │     │  port 4929   │     │   ingest     │
└──────┬───────┘     └──────────────┘     └──────────────┘
       │
       │ port 3902 (web endpoint)
       ▼
  CVMFS clients read from here
```

**Services:**

| Service | Description |
|---------|-------------|
| `garage` | [Garage](https://garagehq.deuxfleurs.fr/) S3-compatible object store (v2) |
| `garage-setup` | One-shot init container: assigns the node to a zone, creates the S3 key and bucket, enables web access |
| `gateway` | Runs `cvmfs_server mkfs -P -D` on first boot, then starts `cvmfs_gateway` |
| `publisher` | Runs `cvmfs_server connect-gw -P` on first boot, then sleeps waiting for ingest commands |

### Running the stack

```bash
cd test/common/container/publish-mountless

# Build and start all services
docker compose build
docker compose up -d
```

Wait for the publisher to be ready:

```bash
docker compose logs -f publisher
# Look for: "Publisher ready"
```

### Ingesting content

Copy a tarball into the publisher container (or mount a volume), then run:

```bash
docker compose exec publisher \
    cvmfs_server ingest -t /path/to/content.tar -b /cvmfs/test.repo.org/
```

### Reading the repository

The gateway container runs `cvmfs_server mkfs -P -D`; the `-D` flag
publishes a self-contained client setup script to the storage backend.
On any machine with the CVMFS client installed, a single `curl | sh`
downloads the repository's public key, writes the client configuration,
and mounts the repository:

```bash
curl http://<DOCKER_HOST>:3902/mount-test.repo.org.sh | sudo sh
# -> Repository is available at /cvmfs/test.repo.org
```

The script configures `CVMFS_SERVER_URL` and `CVMFS_PUBLIC_KEY`
automatically.

Alternatively, configure the client manually:

```ini
# /etc/cvmfs/config.d/test.repo.org.conf
CVMFS_SERVER_URL=http://<DOCKER_HOST>:3902
CVMFS_PUBLIC_KEY=/etc/cvmfs/keys/test.repo.org.pub
```

```ini
# /etc/cvmfs/default.local (if not already present)
CVMFS_HTTP_PROXY=DIRECT
```

The Docker network alias `cvmfs.web.garage.internal` resolves to the Garage
container within the Compose network; from outside, use the host's IP/port.

### Credentials

All credentials are configured via environment variables in
`docker-compose.yml` and have defaults suitable for local testing:

| Variable | Default | Used by |
|----------|---------|---------|
| `S3_ACCESS_KEY` | `GK00c4f5e0a1b2c3d4e5f60011` | garage-setup, gateway |
| `S3_SECRET_KEY` | `00c4f5e0...` (64 hex chars) | garage-setup, gateway |
| `S3_BUCKET` | `cvmfs` | all |
| `GW_KEY_ID` | `mykey` | gateway, publisher |
| `GW_KEY_SECRET` | `mysecret` | gateway, publisher |

### Configuration files

| File | Purpose |
|------|---------|
| `config/garage.toml` | Garage daemon configuration (ports, replication, admin token) |
| `config/repo.json` | Gateway repository list (`["test.repo.org"]`) |
| `config/user.json` | Gateway runtime settings (lease time, receiver path, port) |
| `scripts/setup_garage.sh` | Garage initialisation (zone layout, key import, bucket + website) |
| `scripts/entrypoint-gateway.sh` | Gateway container entrypoint (S3 config, mkfs, start gateway) |
| `scripts/entrypoint-publisher.sh` | Publisher container entrypoint (connect-gw, sleep) |

### Testing direct-to-S3 ingest with the stack

To test the direct-to-S3 data path, copy the S3 config into the publisher
container so the ingest command detects it:

```bash
# Create the S3 config on the publisher (credentials must match Garage)
docker compose exec publisher bash -c 'cat > /etc/cvmfs/test.repo.org.s3.conf << EOF
CVMFS_S3_HOST=cvmfs-garage:3900
CVMFS_S3_ACCESS_KEY=GK00c4f5e0a1b2c3d4e5f60011
CVMFS_S3_SECRET_KEY=00c4f5e0a1b2c3d4e5f6001100c4f5e0a1b2c3d4e5f6001100c4f5e0a1b2c3d4
CVMFS_S3_BUCKET=cvmfs
CVMFS_S3_DNS_BUCKETS=false
CVMFS_S3_MAX_NUMBER_OF_PARALLEL_CONNECTIONS=10
CVMFS_S3_FLAVOR=awsv4
CVMFS_S3_REGION=garage
EOF'

# Now ingest — data goes directly to S3, catalogs through the gateway
docker compose exec publisher \
    cvmfs_server ingest -t /path/to/content.tar -b /cvmfs/test.repo.org/
```

### Cleanup

```bash
docker compose down -v   # -v removes named volumes (Garage data, spool dirs)
```
