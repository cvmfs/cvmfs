# Tolerating hardlinks with missing targets in tarball ingestion

## Problem

`cvmfs_ducc` ingests each OCI/Docker image layer independently into its own
isolated subtree `.layers/<digest>/layerfs` via `cvmfs_server ingest`. Some
layers (e.g. `dnf clean`-style cache-cleaning layers) contain many identical
0-byte files — whiteout markers (`.wh.*`) and empty cache placeholders — that
the image exporter deduplicates into **hardlinks pointing at a single canonical
0-byte member**. When that canonical member lives in a *lower* layer, the layer
being ingested contains a hardlink whose target is not present in its own tar
(a cross-layer / dangling hardlink).

The tarball engine resolves every tar hardlink in `PostUpload` by cloning it
from its target (`WritableCatalogManager::Clone` → `LookupPath`). A missing
target aborted the whole ingest:

```
PANIC: catalog_mgr_rw.cc : catalog for file
  '.layers/47/4714.../layerfs/var/cache/dnf/.../comps.xml.gz'
  cannot be found, aborting
terminate called after throwing an instance of 'ECvmfsException'
```

The crash is a deterministic property of the offending layer's tar; it was not
reproducible on a blank test repo only because that conversion ingested a
different (self-consistent) image build, not because of repo state. (DUCC also
skips a layer entirely if `.layers/<digest>/layerfs` already exists, so a
half-failed ingest never commits and re-crashes on every run.)

## Fix

Add an **opt-in** flag that makes the tarball engine tolerate a hardlink whose
target is absent from the archive by **materializing an empty regular file** at
the link's path instead of aborting. The default behavior of every other caller
(`cvmfs_server publish`, etc.) is unchanged — they still abort.

- `WritableCatalogManager::Clone` gains `fail_if_source_missing` (default
  `true`). When `false`, a missing source returns `false` instead of `PANIC`.
- `SyncUnionTarball::PostUpload`: on a missing target it creates a
  `SyncItemDummyFile` (a new empty-content `SyncItem`, analogous to the
  `.cvmfscatalog` marker) at the link path, preserving the link's
  ownership/permissions, and pushes it through the normal ingestion pipeline so
  the empty object is compressed, hashed, and uploaded consistently. The link's
  `mode/uid/gid/mtime` are captured from the tar header at processing time
  (the `archive_entry` is reused, so they cannot be read later).
- `SyncMediator::Commit` waits for these late uploads before unregistering the
  spooler listeners, so the materialized files' catalog entries are added.
- Plumbed end to end: `swissknife ingest -m` →
  `cvmfs_server ingest -m / --tolerate-missing-hardlinks` →
  `cvmfs_ducc` passes `--tolerate-missing-hardlinks` on every layer ingest.

A fabricated catalog entry with a null hash is not enough: the client rejects
null hashes (`fetch.cc` → `-EIO`), so the empty object must actually exist in
storage — hence materialization goes through the spooler rather than just
writing a directory entry.

## Files touched

- `cvmfs/catalog_mgr_rw.{h,cc}` — `Clone` returns `bool`, optional non-fatal.
- `cvmfs/sync_mediator.{h,cc}` — propagate the flag; wait for late uploads.
- `cvmfs/sync_union_tarball.{h,cc}` — capture link stat; materialize on miss.
- `cvmfs/sync_item_dummy.h` — new `SyncItemDummyFile`.
- `cvmfs/directory_entry.h` — friend declaration for `SyncItemDummyFile`.
- `cvmfs/swissknife_ingest.{h,cc}`, `cvmfs/swissknife_sync.h` — `-m` flag.
- `cvmfs/server/cvmfs_server_ingest.sh`, `cvmfs/server/cvmfs_server_util.sh` —
  `--tolerate-missing-hardlinks` option and usage.
- `test/unittests/mock/m_sync_mediator.h` — updated mock signature.
- `ducc/lib/image.go` — pass the flag when ingesting layers.
