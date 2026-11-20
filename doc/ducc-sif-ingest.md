# DUCC: streaming ingestion of SIF images

Status: draft implementation spec

## 1. Goal

Publish the root filesystem of an Apptainer/Singularity `.sif` image into a
CVMFS repository as an unpacked ("flat") directory tree. This must work:

- on a publisher whose local disk is much smaller than the image;
- with the `.sif` read over the network (HTTP(S) and `oras://`), without
  downloading it first;
- through the normal publish path (`cvmfs_server ingest`), including mountless
  gateway publishers.

The result must be usable exactly like the flat images DUCC already produces
from OCI images: `apptainer exec /cvmfs/<repo>/<public path> ...`.

### Non-goals (v1)

- Encrypted SIF partitions, ext3 partitions, overlay partitions. These are
  rejected with a clear error.
- Signature verification of the SIF (see §12, later phase).
- Writing anything back to the SIF or the source.
- Splitting a SIF into OCI-style layers. A native SIF is one squashfs, so it
  becomes one flat tree.

## 2. Why the current approach is expensive

Today's route is `apptainer build --sandbox` followed by a publish of the
sandbox. Peak local disk, for a SIF of compressed size `S` and unpacked size
`U` (typically `U ≈ 2–4 × S`):

| Copy                                   | Size |
|----------------------------------------|------|
| the `.sif` file                        | `S`  |
| the sandbox directory                  | `U`  |
| overlay upper dir during transaction   | `U`  |
| staged/compressed objects (local backend) | `≈ S` |

That is roughly `S + 2U` before the objects, all of it on the publisher.

The new path has no full copy at any stage:

```
 source (.sif)        ducc                                 cvmfs_server ingest
 ─────────────   ──────────────────────────────────    ───────────────────────
 local file  ┐   SIF header ─► squashfs partition
 https://    ├─► RangeReaderAt ─► squashfs reader ─► tar writer ─► stdin ─► tarball engine
 oras://     ┘   (chunk cache,      (in-memory      (ordered       (-t -)   ─► spooler ─► storage
                  readahead)         index)          entries)
```

Local disk use becomes: catalog SQLite files in the spool directory (they grow
with the number of entries, not data volume), plus whatever the storage backend
keeps locally. Memory use is bounded by configurable caches (§9).

**Caveat:** with a local storage backend (`/srv/cvmfs/<repo>`) the compressed
objects still land on the publisher, because that *is* the repository. The
small-disk property only holds for gateway or S3 upstreams. The docs must
say so.

## 3. Design decision

Implement the SIF/squashfs → tar conversion in Go inside DUCC and feed it to
the existing tarball ingestion engine (`cvmfs_server ingest -t -`). No cvmfs
C++ changes are needed for v1.

Reasons:

- The tarball engine already streams: `SyncUnionTarball` reads one archive
  entry at a time and pipes file data straight into the ingestion pipeline
  (`read_archive_signal_`), so no temporary files are created.
- It creates missing parent directories itself (`CreateDirectories`), so tar
  entry order is free. We use that to order entries by on-disk position in the
  squashfs (§6.2), which turns network access into a nearly sequential read.
- It resolves hardlinks after upload (`PostUpload` → `Clone`), so hardlink
  order does not matter either.
- It honours `.cvmfscatalog` members (`to_create_catalog_dirs_`), so DUCC can
  place nested catalogs.
- It works for both mounted and mountless publishers. `cvmfs/mountless.go`
  already relies on this.

Alternatives considered:

| Option | Why not (now) |
|---|---|
| `apptainer build --sandbox` + publish | The status quo; costs `S + 2U` of disk. |
| `squashfuse` mount + tar of the mountpoint | Needs FUSE and a local seekable file. No control over read order. |
| `sqfs2tar` / `unsquashfs` piped into ingest | Needs a local file. Directory-order reads are random over the network. No catalog placement. Useful as a **benchmark and correctness reference** (§13). |
| Native `SyncUnionSquashfs` in cvmfs (libsquashfs) | Best throughput: no tar serialisation, could reuse data blocks. Much bigger change, tied to cvmfs releases. Keep the source abstraction so this can replace the tar stage later (§14, M6). |

## 4. Package layout

```
ducc/sif/
  source.go        // Source interface, Open(url) dispatch
  source_file.go   // local file
  source_http.go   // HTTP(S) range reader + chunk cache + readahead
  source_oras.go   // oras:// resolution to a blob URL (reuses lib/ registry auth)
  sif.go           // SIF header/descriptor parsing, partition selection
  squashfs/
    superblock.go
    metadata.go    // metadata block reader + cache
    inode.go
    dir.go
    fragment.go
    data.go        // data block reader, sparse handling
    compress.go    // compressor registry
    index.go       // in-memory filesystem index
  tarstream.go     // ordered tar emission
  ingest.go        // glue to cvmfs.IngestWithLogger, repo layout, idempotency
ducc/cmd/convert_sif.go
```

## 5. Input sources

### 5.1 Interface

```go
type Source interface {
    io.ReaderAt
    Size() int64
    // Identity is a stable version identifier of the bytes (ETag,
    // Last-Modified+size, oras digest, or mtime+size+inode for files).
    // Used for change detection, not for content addressing.
    Identity() string
    // KnownDigest returns a sha256 of the whole SIF when the source
    // provides one (oras manifest, user flag); "" otherwise.
    KnownDigest() string
    Stats() SourceStats // bytes fetched, requests, cache hits, retries
    Close() error
}
```

`Open(ref string, opts SourceOptions) (Source, error)` dispatches on the scheme:

- `/abs/path`, `file://` → `os.File`. No cache, the page cache is enough.
- `http://`, `https://` → `httpRangeSource`.
- `oras://<registry>/<repo>:<tag>[@digest]` → resolve to a blob URL, then
  `httpRangeSource`.
- `-` (stdin) is **not** supported. Squashfs keeps its metadata tables at the
  end of the image, so a forward-only stream cannot be converted without
  spooling. Fail with a message that suggests a range-capable URL.

### 5.2 HTTP range reader

- Size probe: `GET` with `Range: bytes=0-0`, parse `Content-Range`. This
  also proves range support. A server that answers `200` to a range request
  is unsupported: fail (§5.4 is the workaround).
- Reads are split into aligned chunks (`--chunk-size`, default 8 MiB), each
  fetched with `Range: bytes=a-b`. The `ETag` and `Last-Modified` of every
  `206` must equal those of the probe; otherwise the object changed
  mid-ingest and the conversion aborts. Never mix bytes from two versions.
  `If-Range` is not used: it needs a strong validator, and Apache treats a
  weak ETag, or a `Last-Modified` less than a second before `Date`, as weak.
  It then answers `200` to every request for a file that was just copied
  into place (seen on cvm-perf10).
- LRU cache of whole chunks (`--cache-size`, default 256 MiB). Concurrent reads
  of the same chunk are coalesced (singleflight).
- Readahead: when two consecutive chunk misses are adjacent, prefetch the next
  `--readahead` chunks (default 4) in parallel. Prefetched chunks go into the
  same LRU.
- Retries: exponential backoff on network errors, 5xx and 429. Follow the
  existing `handleRateLimitBackoff` behaviour in `lib/image.go`. Default 5
  attempts per chunk.
- Redirects: follow them. Registries redirect blob GETs to presigned object
  storage URLs. Keep the original `Authorization` header off cross-host
  redirects.
- Auth: optional `--http-header 'Name: value'` (repeatable) and `.netrc`.
- Stats: log `bytes_fetched / partition_size` at the end. The target is
  ≤ 1.1 for the ordering in §6.2. This number is the main health indicator of
  the streaming design.

### 5.3 oras://

- Resolve the manifest with the existing registry client code (`lib/image.go`:
  token handling, `SetupRegistries` credentials).
- Accept a manifest with exactly one layer of media type
  `application/vnd.sylabs.sif.layer.v1.sif`. Take its `digest` and `size`.
- Blob URL is `<base>/v2/<repo>/blobs/<digest>`. Refresh bearer tokens on
  `401` during long ingests (tokens expire).
- `KnownDigest()` = layer digest. `Identity()` = layer digest.

### 5.4 Spool fallback (optional, M3)

`--spool-dir DIR`: for servers without range support, download to a temporary
file first. This trades disk for compatibility. It is off by default because it
defeats the purpose of the feature.

## 6. Parsing

### 6.1 SIF

- Parse the global header and descriptor table from the start of the source.
  Use `github.com/apptainer/sif/v2` if its loader can run read-only on top of
  an `io.ReaderAt`. Otherwise write a minimal parser (header + fixed-size
  descriptors, ~200 lines): the format is small and stable, and DUCC needs
  only read access.
- Select the partition: data type *partition*, partition type *primary
  system*. Exactly one is expected.
  - FS type squashfs → proceed.
  - FS type ext3 or encrypted squashfs → error `unsupported SIF partition
    type`.
  - No primary partition but OCI root index/blob descriptors present → OCI-SIF
    (§12). In v1, error `OCI-SIF not supported yet`.
- Record for metadata (§8.4): SIF ID (UUID), creation time, partition
  architecture, the list of descriptor types and sizes, and whether signature
  descriptors exist.
- Hand `io.NewSectionReader(src, part.Offset, part.Size)` to the squashfs
  reader.
- A bare `.squashfs` file (magic `hsqs` at offset 0) is accepted as well and
  skips the SIF layer.

### 6.2 Squashfs reader

A read-only squashfs 4.0 reader written for this use case. Reasons not to use a
general-purpose library (e.g. `CalebQ42/squashfs`, `go-diskfs`):

- we need inode-level access to `blocks_start` / fragment index to sort entries
  by data position;
- we need control over every read for caching and readahead accounting;
- we need parallel, in-order block decompression.

The library is still fine for a quick prototype and as a test cross-check.

Format handling:

- **Superblock** (96 bytes): check the magic, check version 4.0, read block
  size, compressor id, flags and table offsets. If the compressor-options flag
  is set, read the compressor options metadata block (needed for some xz/lz4
  configurations).
- **Compressors**: gzip (zlib stream, stdlib `compress/zlib`), xz
  (`ulikunitz/xz`), lzma (`ulikunitz/xz/lzma`), lz4 (`pierrec/lz4/v4`), zstd
  (`klauspost/compress/zstd`). lzo is rejected (rare, cgo-only Go bindings).
  Decoders are pooled per worker.
- **Metadata blocks**: 2-byte header, bit 15 means uncompressed, at most 8 KiB
  uncompressed. Inode references are `(block_offset << 16) | offset`. Cache
  decompressed metadata blocks keyed by their absolute offset.
- **Inodes**: all 14 types (basic and extended dir, file, symlink, blk, chr,
  fifo, socket). Extended file inodes carry `nlink`, `sparse` and `xattr`.
- **Directories**: walk the directory table from the root inode, depth first.
  Handle the header/entry structure (inode number base + delta, inode
  reference reconstruction).
- **Fragment table**: two-level (lookup table of metadata block pointers, then
  16-byte entries `{start, size}`). Load it fully; it is small.
- **Id table**: load fully; it maps uid/gid indices to numeric ids.
- **Xattr table**: parse it, to count and report dropped xattrs (§7.4).
- **Data blocks**: 32-bit size words. Bit 24 means stored uncompressed; size 0
  means a sparse block, emitted as zeros (the last block may be short). Tail
  data comes from the fragment block at `fragment_offset`.

Validation: bound every offset and length against the partition size. Reject
cycles in the directory graph. Cap the name length at 256. Cap a decompressed
block at the block size. A corrupt or malicious image must give an error,
never a panic or unbounded allocation.

### 6.3 In-memory index

One pass over the inode and directory tables builds:

```go
type entry struct {
    parent   uint32     // index into entries
    name     strRef     // into a shared name arena
    inode    uint32     // squashfs inode number (hardlink key)
    mode     uint32
    uid, gid uint32
    mtime    uint32
    size     uint64
    blkStart uint64     // 0 if no full blocks
    blkList  blkRef     // into a shared []uint32 arena
    frag     uint32     // 0xFFFFFFFF if none
    fragOff  uint32
    link     strRef     // symlink target
    rdev     uint32
}
```

Expected cost is about 80–120 bytes per entry plus names. A 1M-entry image
needs about 150 MB. Log the entry count and the index size. `--max-entries`
(default 10M) guards against hostile images.

## 7. Tar stream

### 7.1 Ordering

1. All directories in pre-order. These come from metadata only, so this phase
   reads no data blocks.
2. All non-directory entries, ordered by **data position**:
   - entries without data (symlinks, devices, fifos, sockets, empty files)
     first;
   - then regular files by `blkStart`, or by fragment block start if the file
     has no full blocks; ties broken by `fragOff`.

mksquashfs writes file data in traversal order and flushes fragment blocks as
they fill, so this order reads the data region nearly front to back. Small
files that share a fragment block become adjacent, which keeps the fragment
cache hot. A file's tail fragment can sit ahead of its blocks; the chunk cache
plus readahead absorbs that.

Directory mtimes: the tarball engine takes directory metadata from the
directory's own tar header, emitted in phase 1, so adding children later does
not change it. Verify this in tests (§13). If it does not hold, re-emit
directory headers at the end.

### 7.2 Entry mapping

| squashfs           | tar                                                        |
|--------------------|------------------------------------------------------------|
| dir                | `TypeDir`                                                  |
| file               | `TypeReg`, size from the inode, data from blocks + fragment |
| symlink            | `TypeSymlink`                                              |
| chr / blk          | `TypeChar` / `TypeBlock` with major/minor from `rdev`      |
| fifo               | `TypeFifo`                                                 |
| socket             | skipped and counted (tar has no socket type; a socket in an image is always stale) |
| 2nd+ path of an inode | `TypeLink` to the first emitted path of that inode       |

- Use `tar.FormatPAX` for names over 100 bytes, large sizes and
  high-resolution fields. Do not emit PAX xattr records (§7.4).
- Names are relative to the ingest base dir (no leading `/`). Reject names
  containing `..` components or NUL, as defence in depth: squashfs names cannot
  contain `/`.
- uid/gid come from the id table and are kept: ingest runs with
  `--keep-ownership`. Without it every entry gets the repository owner's
  uid/gid, which breaks e.g. `root:tty` on setgid `/usr/bin/write`. Layer
  ingestion still uses the repository owner; whether it should change too is
  a separate question.
- Hardlinks: key on the squashfs inode number. The first path in emission
  order (§7.1) carries the data. Later paths become `TypeLink`. Every target
  is in the same stream, so `--tolerate-missing-hardlinks` is not passed.
  This keeps SIF ingestion working with cvmfs-server releases that lack that
  flag.

### 7.3 Data pipeline

```
planner ──(ordered block jobs)──► fetch+decompress pool (N workers)
                                          │  results re-sequenced by job id
                                          ▼
                              tar writer (single goroutine) ──► io.Pipe ──► ingest stdin
```

- The planner expands the ordered entries into block jobs: `(entry, block i,
  offset, csize, uncompressed?)` and fragment jobs.
- Workers read through the source and decompress. Outstanding work is capped
  by bytes (`--decompress-window`, default 64 MiB), not job count, so memory
  stays bounded when blocks are large (block size up to 1 MiB).
- Fragment blocks go into their own LRU of decompressed blocks (default 64
  entries), because many files reference each one.
- The tar writer writes blocks in order. Backpressure from the ingest stdin
  pipe throttles the whole chain.
- Errors at any stage close the pipe with `CloseWithError`, so ingest gets a
  truncated archive and aborts. This is the pattern already used in
  `cvmfs/mountless.go:ingestTarStreamWithLogger`.

### 7.4 Things that are lost or changed

- **xattrs**: the tarball engine does not import xattrs today (no xattr
  handling in `sync_item_tar.cc` / `sync_union_tarball.cc`). Count them per
  namespace and log a warning with the count. `security.capability` is the
  one that matters (e.g. `ping`). Whether any target image needs them is
  unknown: leave a `TODO(xattr)` at the point in `tarstream.go` where they
  are dropped, noting that emitting PAX `SCHILY.xattr.*` records requires
  tarball-engine support first.
- **Sockets**: skipped (above).
- **Hardlinks** become independent catalog entries with the same content hash
  (cvmfs `Clone`). Content is deduplicated in storage; `st_nlink` differs.
  This matches layer ingestion.

### 7.5 Nested catalogs

The full tree is known before the first byte is emitted, so catalogs can be
placed exactly:

- Bottom-up subtree weights (entry counts) from the index.
- Place a `.cvmfscatalog` member in a directory when its remaining subtree
  weight reaches `--catalog-max-weight`. Do not create catalogs below
  `--catalog-min-weight`. Default to the same values as the server's
  autocatalog settings (`CVMFS_AUTOCATALOGS_MAX_WEIGHT` /
  `CVMFS_AUTOCATALOGS_MIN_WEIGHT`; confirm the current defaults in
  `cvmfs_server_mkfs.sh`).
- The root of the image always gets a catalog via `ingest --catalog`, as for
  layers.
- Never place a marker where the image already has a file named
  `.cvmfscatalog`. If the image contains one, log it; it will create a catalog,
  which is harmless.

## 8. Publishing

### 8.1 Image identity and repository layout

The ingest target path must be known before ingest starts, because cvmfs
ingest cannot rename. Image ID (64 hex chars):

1. `--digest sha256:HEX` (CLI) / `digest:` (wish list) if given: explicit
   override.
2. Else the oras layer digest: the sha256 of the whole SIF.
3. Else (default) `sha256( SIF global header ‖ descriptor table )`. This
   covers the SIF UUID, creation time and every partition's offset, size and
   type. It is cheap (one small read) and changes on every rebuild. It is not
   a content hash; `--verify-digest` (§8.5) gives content guarantees when an
   explicit or oras digest is available.

A bare `.squashfs` has no descriptor table, so `--digest` is required for it.

Layout, compatible with the existing flat-image GC (`FindAllFlatImages`
expects `.flat/<xx>/<id>` directories; `FindAllUsedFlatImages` follows public
symlinks):

```
/cvmfs/<repo>[/<subdir>]/
  .flat/<id[0:2]>/<id>/            ← ingest -b target, catalog root
  .sifinfo/<id[0:2]>/<id>.json     ← provenance (§8.4)
  <public path>                    ← symlink → .flat/<xx>/<id>
```

Public path: every SIF-sourced image lives under a fixed `sif/` prefix, so
SIF images never collide with registry-sourced ones:

- `oras://reg/repo:tag` → `sif/<reg>/<repo>:<tag>`;
- HTTP or file sources → `--name` is required and is placed under `sif/`
  (`--name lhcb/analysis:v3` → `sif/lhcb/analysis:v3`). No guessing from URLs.
  Reject names that are absolute or contain `..`.
- With `--multi-arch`, use the partition architecture → the
  `.multiarch/<arch>/...` scheme of `ConvertWishFlat` / variant symlinks.

### 8.2 Sequence

1. Open the source, parse the SIF, compute the ID.
2. If `/cvmfs/<repo>/.flat/<xx>/<id>/.singularity.d` exists → skip to step 6,
   unless `--force`. This is the same completeness check as `ConvertWishFlat`.
   In mountless mode use the same existence check the flat path uses there.
3. Build the index (§6.3). Abort on validation errors before touching the
   repo.
4. Check that `/.singularity.d` exists in the index. A native SIF always has
   it. If it is missing, fail unless `--allow-missing-singularity-d`.
5. `cvmfs.CreateCatalogIntoDirWithLogger(repo, ".flat")`, then
   `cvmfs.IngestWithLogger(repo, pipeReader, "--catalog", "--keep-ownership",
   "-t", "-", "-b", ".flat/<xx>/<id>")`. The subdir prefix is applied by
   `PrefixRepoSubdirOnce`, as for layers. With `--force`, fast-delete the old
   tree first: ingesting over an existing tree keeps the old metadata of
   directories that already exist.
6. Write `.sifinfo/<xx>/<id>.json` (`WriteDataToCvmfsWithLogger`).
7. Create or update the public symlink (`CreateSymlinkIntoCVMFSWithLogger`,
   which already handles mountless publishers). Skip this if the link already
   points at the target, so a no-op run publishes no revision. A new image
   currently costs three revisions: ingest, `.sifinfo`, symlink.
8. Send notifications `start_sif_ingest` / `end_sif_ingest` with elapsed time,
   bytes fetched, entries, the dropped-xattr count and the source identity.
   Reuse `notification.NewNotification`.

### 8.3 Failure handling

- The ingest is one transaction, so a failure publishes nothing at the target.
  Objects uploaded before the failure are left for `cvmfs_server gc`.
- On a non-zero ingest exit, run `IngestFastDeleteWithLogger` on the target,
  best effort, as `IngestIntoCVMFSWithLogger` does for layers.
- If the source changes mid-ingest (`If-Range` mismatch), abort and retry the
  whole conversion once, up to `--attempts` (same semantics as
  `convert-single-image -r`).
- Ctrl-C / SIGTERM: cancel the context. The tar writer closes the pipe with an
  error and ingest aborts its transaction or lease.

### 8.4 Provenance file

```json
{
  "schema": 1,
  "source": "https://example.org/images/foo.sif",
  "source_identity": "\"etag-abc\"",
  "image_id": "…",
  "id_kind": "oras-digest | sif-descriptors | user-digest",
  "sif": { "uuid": "…", "created": "…", "arch": "amd64",
           "signed": true, "descriptors": [{"type": "…", "size": 0}] },
  "squashfs": { "compressor": "zstd", "block_size": 131072,
                "inodes": 0, "bytes_used": 0 },
  "dropped": { "xattrs": 0, "sockets": 0 },
  "ducc_version": "…",
  "ingested_at": "…"
}
```

The daemon uses this for change detection (§10.2). Operators use it for
debugging.

### 8.5 Optional content verification

`--verify-digest`: hash the whole SIF in a separate sequential stream that
runs alongside ingest, and compare it with `KnownDigest()` before step 7. On
mismatch the image is ingested but not linked, and the error is reported.
Cost: one extra full read of the source. Off by default.

### 8.6 Locking and gateway leases

- Today `IngestWithLogger` takes the global DUCC lock (`getLock`: an
  in-process mutex plus the `/tmp/DUCC.lock` file lock) for the whole ingest,
  so a multi-hour SIF ingest would block every other DUCC write on the host.
- **Gateway publishers: parallel across nodes, not within one.**
  `cvmfs_server ingest` itself serialises ingests per repository on a
  publisher node, both mounted and mountless. It holds
  `${CVMFS_SPOOL_DIR}/is_publishing` for the whole `cvmfs_swissknife ingest`
  run (non-blocking `acquire_lock`; a second ingest dies with "Failed to
  acquire publishing lock"). It also uses the single per-repo
  `${spool_dir}/session_token` and `${spool_dir}/tmp/manifest`. A ducc-side
  per-lease-path lock was implemented and reverted for this reason: it would
  only turn waiting into failures.
  - Parallelism today: run ducc on several gateway publisher nodes. The DUCC
    lock is node-local (`/tmp/DUCC.lock`), and each ingest leases only
    `.flat/<xx>/<id>`, so nodes do not block each other.
  - Parallelism on one node needs cvmfs changes: per-lease spool state
    (session token, manifest, publish lock keyed by lease path). Hand this to
    the cvmfs side together with §9.1.
- Gateway lease lifetime for long ingests is assumed to be handled by
  `cvmfs_server ingest`; not covered here.

## 9. Resource bounds

| Resource | Bound | Knob |
|---|---|---|
| HTTP chunk cache | 256 MiB | `--cache-size` |
| Readahead | 4 × 8 MiB | `--readahead`, `--chunk-size` |
| Decompression window | 64 MiB | `--decompress-window` |
| Fragment cache | 64 × block size | `--fragment-cache` |
| Metadata block cache | 32 MiB | (internal) |
| Index | ~150 B/entry | `--max-entries` |
| Local disk (DUCC) | 0 | — |
| Local disk (cvmfs) | catalogs in spool tmp, plus backend if local | see §9.1 |

### 9.1 Spool usage of the ingest transaction

With ducc using no local disk, what remains is the cvmfs side of the
transaction under `/var/spool/cvmfs/<repo>/`:

- **Catalogs**: the writable catalog manager keeps every touched catalog as a
  SQLite file in spool tmp until commit. For a 1M-entry image this grows with
  the entry count. In principle only the catalog currently being written
  needs to be on disk: once the tar stream has moved past a nested catalog's
  subtree (§7.1 emits directories first, so this needs a catalog-aware
  ordering), that catalog could be finalised, uploaded and dropped.
- **Overlay**: on mounted publishers `cvmfs_server ingest` still opens a full
  overlay transaction (scratch dir, rw union mount), although the tarball
  engine does not write file data there. Tarball ingestion may not need the
  rw overlay at all; the mountless path shows ingest can run without FUSE.
- **Object staging**: depends on the upstream (local uploader temp files,
  gateway object packs, S3).

This is investigated separately, with fixes to cvmfs, on branch
`ingest-spool-usage` (`doc/ingest-spool-disk-usage.md` there). If early
catalog finalisation lands, §7.1 and §7.5 must change: emit each nested
catalog's subtree contiguously (directories and files together), then data
ordering applies within a subtree only. Re-measure the fetch ratio (§5.2) for
that ordering.

Until then, measure the actual spool usage in M2 on a gateway publisher: peak
of `/var/spool/cvmfs/<repo>/tmp` against the entry count. Put the numbers in
the user docs.

## 10. CLI and wish-list integration

### 10.1 Command

```
cvmfs_ducc convert-sif <source> <cvmfs repository> [flags]

  --name PATH               public path under sif/ (required for http/file sources)
  --digest sha256:HEX       override the image ID (default: SIF header+descriptor hash)
  --verify-digest           hash the source and compare before linking
  --force                   re-ingest even if .flat/<xx>/<id> is complete
  --attempts N              whole-conversion retries (default 1)
  --multi-arch              publish under .multiarch/<arch>/ + variant symlink
  --max-parallel-sif N      concurrent SIF ingests on gateway publishers (default 2)
  --cache-size, --chunk-size, --readahead, --decompress-window, --fragment-cache
  --catalog-max-weight, --catalog-min-weight
  --http-header 'K: V'      repeatable
  --spool-dir DIR           download first (for servers without range support)
  --emit-tar FILE|-         write the tar stream and exit; no repository access
  --dry-run                 parse + index + print summary; no repository access
```

`--emit-tar` and `--dry-run` matter for testing and operations. They let you
check an image, or compare the stream with `unsquashfs`, without a publisher.

The command respects the existing global `--mountless` /
`CVMFS_DUCC_MOUNTLESS_PUBLISHING`.

### 10.2 Wish list

Extend `YamlRecipeV1` with an optional section. Existing recipes stay valid:

```yaml
version: 1
user: cvmfs
cvmfs_repo: unpacked.example.org
input:
  - https://registry.hub.docker.com/library/ubuntu:24.04
sif_input:
  - source: oras://ghcr.io/org/analysis:v3
  - source: https://images.example.org/sif/sim-2026.sif
    name: example/sim:2026      # published as sif/example/sim:2026
    digest: sha256:…            # optional image ID override
```

The daemon loop (`cmd/loop.go`) re-resolves each `sif_input` on every
iteration: oras digest, or HTTP `ETag`/`Last-Modified`/size. It re-ingests
only when the identity differs from `.sifinfo/.../<id>.json` for the image the
public symlink currently points to. An unchanged source costs one `HEAD` (or
one manifest GET).

### 10.3 Garbage collection

No new GC logic is needed. When a `sif_input` is removed, or its symlink moves
to a new ID, the old `.flat/<xx>/<id>` is no longer a symlink target and the
existing flat GC removes it. Extend that GC to also delete the matching
`.sifinfo/<xx>/<id>.json`.

## 11. Mountless publishers

Every repository write in §8.2 already has a mountless path: ingest, symlink
via ingest, data file via ingest. The one gap is the existence check in step 2,
which today stats `/cvmfs/<repo>`. In mountless mode, use whatever the
flat-image path uses there. If nothing is available, use `--force`-like
behaviour plus a provenance check through the client mount when one exists.
Integration-test this explicitly (§13).

## 12. OCI-SIF (later phase)

`apptainer --oci` produces SIFs that embed an OCI image: index, manifest,
config and layer blobs as SIF descriptors, with layers stored as squashfs
(`application/vnd.sylabs.image.layer.v1.squashfs`), usually squashed to one
layer.

Plan:

- Parse the OCI root index descriptor, then the manifest and config from blob
  descriptors.
- Single layer: ingest it as a flat tree with the reader above, then generate
  `.singularity.d` from the OCI config with the existing `singularity` package
  (`InsertRunScript`, `InsertEnv`, `MakeBaseEnv`), as an extra tar section at
  the end of the same stream.
- Multiple layers: ingest each squashfs layer into
  `.layers/<xx>/<digest>/layerfs` (OCI whiteouts are stored as such in those
  layers) and reuse `cvmfs_server overlay` + `-c <config>` exactly like
  `CreateFlatOverlayWithLogger`.
- Image ID = OCI config digest, so the result shares `.flat/` with
  registry-sourced images of the same config.

Signature verification (both kinds): verify the SIF signature descriptors
against a configured keyring before linking. Record the fingerprints in
`.sifinfo`.

## 13. Testing

### Unit (Go, in-session)

- **squashfs reader**: fixtures built at test time with `mksquashfs`; skip if
  absent. Use pseudo-file definitions (`-p`) for device nodes, so no root is
  needed. Matrix:
  - compressors gzip/xz/lz4/zstd;
  - block sizes 4 KiB/128 KiB/1 MiB;
  - `-no-fragments`, `-always-use-fragments`, `-no-duplicates`;
  - sparse files, hardlinks (including hardlinked symlinks), 255-byte names,
    deep trees, a directory with more than 256 entries (multiple dir headers),
    empty files, an empty image.
- **Oracle**: compare the `--emit-tar` output with `unsquashfs -lln` for the
  listing, and with the sha256 of every regular file from `unsquashfs -d`
  extraction. The test passes on an exact match of type, mode, uid, gid,
  mtime, size, link target and content.
- **Fuzzing**: `go test -fuzz` on superblock, metadata, inode and directory
  parsing. Must not panic, must not allocate beyond the caps.
- **SIF**: synthetic SIFs written with the sif library in tests (test-only
  dependency), covering squashfs, ext3, encrypted, OCI-SIF and
  missing-partition cases.
- **Range source**: `httptest` server with Range support, injected 5xx/429,
  dropped connections, an `ETag` change mid-read (must abort), a no-Range
  server (must fail without `--spool-dir`), redirects across hosts (no auth
  leak). Assert `bytes_fetched / partition_size ≤ 1.1` for the ordering in
  §7.1 on a realistic fixture.
- **Ordering**: property test that every entry is emitted exactly once,
  directories come before non-directories, and each `TypeLink` points at a
  previously emitted `TypeReg`.

### Integration (test/src, cvmfs test hosts)

- `4xx-ducc-sif-ingest`: build a small SIF with `apptainer build` (or ship a
  tiny fixture). Serve it from the test host's Apache, which supports Range.
  Run `convert-sif` and check:
  - `apptainer exec /cvmfs/<repo>/<name> cat /etc/os-release`;
  - the file listing equals `unsquashfs -lln` (modulo dropped sockets);
  - a second run is a no-op (no new revision);
  - on a gateway publisher, two SIF ingests run concurrently and both land;
  - a changed SIF is re-ingested and re-linked;
  - GC removes the old `.flat` entry.
- Mountless gateway variant of the same test.
- `4xx-ducc-sif-ingest-large` (performance, manual or nightly): a multi-GB
  SIF over HTTP on a publisher with a small `/var/spool` filesystem. Record
  peak spool usage, peak RSS, wall time, and bytes fetched compared with
  `apptainer build --sandbox` + publish of the same image.

## 14. Milestones

| | Scope | Exit criterion |
|---|---|---|
| M1 | squashfs reader, index, tar emitter, local file source, `--emit-tar`, `--dry-run` | Oracle tests green for the full fixture matrix |
| M2 | ingest glue, repo layout, ID, idempotency, nested catalogs, `convert-sif` | Integration test green (mounted and mountless); spool usage measured |
| M3 | HTTP range source, cache, readahead, retries, oras://, `--spool-dir` | Range tests green; ≤ 1.1× fetch ratio on a realistic image |
| M4 | `sif_input` in recipes, daemon change detection, notifications, `.sifinfo` GC | Loop test: change → re-ingest, no change → no-op |
| M5 | OCI-SIF, signature verification | |
| M6 (optional) | Native squashfs sync in cvmfs, using the same ordering ideas | Benchmark gain justifies the C++ change |

## 15. Decisions and open items

1. **Gateway lease lifetime** for multi-hour ingests: assumed handled by
   `cvmfs_server ingest`. Out of scope.
2. **Image ID**: the SIF header+descriptor hash is the default; `--digest` /
   `digest:` overrides it (§8.1).
3. **xattrs**: undecided. `TODO(xattr)` in the code (§7.4).
4. **Locking**: parallel across gateway publisher nodes. Within one node,
   `cvmfs_server ingest` serialises per repository; that needs cvmfs changes
   (§8.6).
5. **Public path**: fixed `sif/` prefix (§8.1).
6. **Spool disk usage** of the ingest transaction: investigated on branch
   `ingest-spool-usage` (§9.1). Open.

## 16. Implementation status

Implemented in `ducc/sif/` (`sif.go`, `squashfs/`, `tarstream.go`,
`source.go`, `ingest.go`) and `ducc/cmd/convert_sif.go`. Deviations from the
sections above are noted there (§5.2 validators, §7.2 ownership, §8.2
`--force` and symlink, §8.6 locking).

- **M1 complete.**
  - Own SIF parser; squashfs 4.0 reader (gzip, xz, lzma, lz4, zstd).
  - Ordered tar emitter with parallel in-order decompression.
  - Nested catalog placement; `--emit-tar`, `--dry-run`.
- **M2 complete** except a mountless or gateway integration run.
  - Ingest glue, layout, image ID, `--digest`.
  - Idempotency: an unchanged rerun publishes no revision.
  - `--force`, provenance file.
- **M3 partly done.** The HTTP range source has a chunk cache, readahead,
  retries and change detection. oras:// and `--spool-dir` are not done.
- **M4, M5:** not started.

### 16.1 Ordering for early catalog upload

Branch `ingest-spool-usage` adds `cvmfs_server ingest --contiguous-catalogs`.
With it, a nested catalog is uploaded and dropped from the spool as soon as
the stream leaves it. ducc's order therefore became:

1. All directories, parents first.
2. Depth first over the catalog tree: each catalog's marker (synthetic, or
   the image's own regular `.cvmfscatalog`), then its entries. Entries
   without data come first, then files by data position within the
   catalog.

`--contiguous-catalogs` on `convert-sif` passes the flag through. It also
restricts tar hardlinks to the same or an enclosing catalog; otherwise the
path is emitted with its data, and storage deduplicates it. The grouped
order is used in both modes.

### 16.2 Measurements (cvm-perf10, local upstream)

| | alma9 SIF | PyTorch SIF |
|---|---:|---:|
| SIF size | 70 MB | 3.82 GB |
| unpacked | 194 MB | 8.06 GB |
| entries / hardlinks | 9,387 / 1,488 | 102,731 / 39,149 |
| ingest time over HTTP | 2.6 s | 93 s |
| fetch ratio (bytes fetched / SIF size) | 1.001 | 1.002 |
| tar stream alone (`--emit-tar /dev/null`) | — | 2.8 s (64 workers), 9.9 s (8) |

The ingest is bound by `cvmfs_server ingest`, not by ducc.

**PyTorch SIF: streaming vs. `apptainer build --sandbox` + transaction +
publish**

| | streaming | sandbox + publish |
|---|---:|---:|
| local copies on the publisher | none (SIF served over HTTP) | SIF 3.8 GB + sandbox 8.07 GB + overlay scratch 8.09 GB |
| spool growth during publish | +34 MB | +8.1 GB |
| wall time | 1 min 35 s | 16 s build + 13 s copy + 79 s publish = 108 s |

**ducc memory (PyTorch, `--emit-tar`)**

- 0.97 GB peak RSS before setting a soft memory limit.
- 0.64 GB with the limit and a 256 MiB chunk cache.
- 0.43 GB with the new default 64 MiB cache, at the same fetch ratio.

**Spool tmp peak (PyTorch, `--catalog-max-weight 20000`, 12 nested
catalogs)**

| cvmfs_server | peak |
|---|---:|
| system 2.14.0 | 37.0 MB |
| `ingest-spool-usage` | 19.0 MB |
| `ingest-spool-usage` + `--contiguous-catalogs` | 6.1 MB |

The two branch runs published hardlinks of chunked files without chunk
lists (EIO on read). `Clone` does not copy chunk lists, and the branch's
enabling of ingest chunking (`956990e21`) made that reachable. A fix is
pending on that branch.

### 16.3 Tests

- `go test ./sif/...`:
  - mksquashfs fixture matrix (11 variants), compared entry by entry with
    the source tree;
  - devices and ownership via pseudo files;
  - SIF wrapper and rejection cases;
  - catalog placement and contiguity, cross-catalog hardlinks;
  - HTTP range source: byte-identical to the file path; change, no-range,
    retry, weak→strong ETag;
  - corruption loop and `FuzzImage` (650k execs, no failure).
- `test/src/409-ducc-sif-ingest`: passes on cvm-perf10 (AlmaLinux 9.8,
  cvmfs-server 2.14.0). It compares with `unsquashfs` (metadata and
  content), runs apptainer, checks that a no-op rerun publishes no
  revision and that a changed SIF is relinked, and checks that stdin is
  rejected.
