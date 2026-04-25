# Publishing Pipeline Performance Improvements

This branch applies a series of targeted performance improvements to the CVMFS
publishing pipeline. All changes are relative to the current `devel` branch.
No existing behaviour, wire format, or on-disk catalog schema is altered.

---

## Overview

The CVMFS publishing path consists of four major phases:

1. **Directory traversal** — union filesystem scratch layer is walked to detect
   added, modified, and removed entries.
2. **Ingestion pipeline** — changed files are read, chunked, compressed, hashed,
   and uploaded to the storage backend concurrently.
3. **Catalog finalization** — writable SQLite catalog databases are updated,
   counters recalculated, and each catalog file is VACUUM-ed and uploaded.
4. **Manifest commit** — the updated root catalog hash is signed and published.

Profiling and code inspection identified eight independent bottlenecks. Each is
addressed by a self-contained change described below.

---

## Change 1 — Parallel directory tree traversal

**Files:** `cvmfs/util/fs_traversal.h`, `cvmfs/sync_union_overlayfs.cc`,
`cvmfs/sync_union_aufs.cc`

### Problem

`FileSystemTraversal<T>::Recurse()` performs a fully serial depth-first walk.
On a scratch layer with tens of thousands of files, `opendir` / `readdir` /
`lstat` calls dominate publish time, and only one directory is processed at a
time. Modern storage (NVMe, network-attached FUSE-over-Ceph) can service many
concurrent metadata requests.

### Solution

A new `RecurseParallel(root_path, num_threads = 0)` method implements a
two-phase design:

- **Phase 1 (parallel):** A work-queue of `DirScanNode` items is processed by
  `num_threads` workers (auto-detected from `sysconf(_SC_NPROCESSORS_ONLN)`
  when 0). Each worker calls `opendir` / `readdir` / `lstat` and pushes child
  directories back into the queue, building an in-memory tree.
- **Phase 2 (serial):** `ReplayTree()` walks the pre-built tree in exactly the
  same DFS order as the original `Recurse()`, firing `fn_enter_dir`,
  per-entry-type, and `fn_leave_dir` callbacks sequentially.

Serial replay is required because `SyncMediator::EnterDirectory` /
`LeaveDirectory` update a shared catalog traversal stack that is not
thread-safe. The two-phase split achieves parallel I/O without touching any
callback concurrency contract.

Both `SyncUnionOverlayfs::Traverse()` and `SyncUnionAufs::Traverse()` are
updated to call `RecurseParallel`.

---

## Change 2 — Parallel leaf catalog finalization

**Files:** `cvmfs/catalog_mgr_rw.h`, `cvmfs/catalog_mgr_rw.cc`

### Problem

`WritableCatalogManager::SnapshotCatalogs()` collects all leaf catalogs (those
with no nested children) and finalizes them in a serial loop. Each
`FinalizeCatalog()` call runs `UpdateCounters`, `Commit`, and
`VacuumDatabaseIfNecessary`. The VACUUM operation rewrites the entire SQLite
file and is strongly I/O-bound — it dominates finalization time. With N leaf
catalogs, the total cost is O(N × VACUUM_time).

### Solution

Leaf catalogs own independent SQLite files with no shared mutable state during
finalization (the one shared read — `parent->FindNested` — is already protected
by `sync_lock_`). A `FinalizeLeafThread` static method is introduced; one
`pthread_t` is created per leaf catalog, all threads are joined before
processing non-leaf catalogs. This turns O(N × VACUUM_time) into
O(max_VACUUM_time) for the leaf tier.

The serial path (one leaf, or `stop_for_tweaks` mode) is preserved unchanged.

---

## Change 3 — Larger ingestion pipeline block sizes

**Files:** `cvmfs/ingestion/task_read.h`, `cvmfs/ingestion/task_compress.h`

### Problem

The read stage dispatches 16 KB `BlockItem` objects into the chunk tube; the
compress stage dispatches 8 KB compressed `BlockItem` objects into the hash
tube. Each dispatch involves a mutex acquire and a condvar signal. For a large
file, a single 16 KB read block produces at least two 8 KB compress dispatches,
meaning three tube round-trips (read→chunk, chunk→compress, compress→hash) for
every 8 KB of data.

### Solution

| Constant | Before | After |
|---|---|---|
| `TaskRead::kBlockSize` | `kPageSize × 4` (16 KB) | `kPageSize × 32` (128 KB) |
| `TaskCompress::kCompressedBlockSize` | `kPageSize × 2` (8 KB) | `kPageSize × 16` (64 KB) |

For typical files, the number of tube dispatches drops by approximately 8×
with no change to correctness or memory safety. The pipeline watermark system
(low/high watermarks on `BlockItem::managed_bytes()`) continues to provide
backpressure.

---

## Change 4 — Hash stage merged into Compress stage

**Files:** `cvmfs/ingestion/task_compress.cc`, `cvmfs/ingestion/pipeline.h`,
`cvmfs/ingestion/pipeline.cc`, `cvmfs/compression/compression.h`

### Problem

The ingestion pipeline has a dedicated `TaskHash` stage between Compress and
Write. Its `Process()` method calls `shash::Update()` on each block and
forwards the block unchanged. This costs a full inter-thread tube dispatch (one
mutex + condvar pair, per block) just to call one OpenSSL function on data that
is already in memory.

### Solution

`TaskHash::Process()` logic is moved inline into `TaskCompress::Process()`:

- After each compressed output block is ready and before it is dispatched to
  `tubes_write_`, `shash::Update()` is called on its data.
- On the flush (stop) block, `shash::Final()` is called after the last data
  block is dispatched, before the stop sentinel is sent downstream.

`TaskHash`, `kNforkHash`, `tubes_hash_`, and `tasks_hash_` are removed from
`IngestionPipeline`. `ScrubbingPipeline` and `CompressHashPipeline` retain
their own independent hash stages and are unaffected.

Additionally, `EchoCompressor` (used when `kNoCompression` is selected) gains
`IsPassthrough() const → true`. `TaskCompress::Process()` short-circuits the
entire allocate-deflate-copy loop for passthrough compressors: `shash::Update`
is called directly on the input block's data buffer, and the block is forwarded
to the write stage with zero copies and zero intermediate buffer allocations.
This benefits repositories storing pre-compressed content (`.tar.gz`, `.ROOT`,
HDF5) where `kNoCompression` is the typical setting.

---

## Change 5 — Upload task parallelism scales with CPU count

**Files:** `cvmfs/upload_spooler_definition.h`,
`cvmfs/upload_spooler_definition.cc`, `cvmfs/swissknife_sync.h`,
`cvmfs/swissknife_sync.cc`

### Problem

`SpoolerDefinition::kDefaultNumUploadTasks = 1`. The ingestion pipeline
compresses and hashes multiple files concurrently across 4–16 workers, but by
default only one upload task ships compressed blocks to the backend. For local
filesystem backends and the gateway uploader, this serialises all I/O writes
regardless of storage throughput. (The S3 uploader already overrides this with
its own 16-connection pool; the gap affects everyone else.)

### Solution

The `num_upload_tasks` field is initialised at `SpoolerDefinition` construction
time using `GetNumberOfCpuCores()`:

```cpp
num_upload_tasks = std::min(16U, std::max(4U, GetNumberOfCpuCores() / 2))
```

This gives 4 upload tasks on a 4-core machine, scales linearly to 16 on a
32-core machine, and caps at 16 to avoid overwhelming backends.

`SyncParameters::num_upload_tasks` is changed from `1` to `0` (sentinel meaning
"inherit from SpoolerDefinition"). The override in `swissknife_sync.cc` is
made conditional: `spooler_definition.num_upload_tasks` is only overwritten
when the operator explicitly passes the `-0` flag.

---

## Change 6 — `synchronous=NORMAL` for publish catalogs

**Files:** `cvmfs/sql_impl.h`

### Problem

`Database<DerivedT>::Configure()` applies pragmas only for read-only opens. For
read-write opens (publish-path writable catalogs) no pragmas are set, so SQLite
uses its defaults: `synchronous=FULL`. This calls `fsync()` after every
transaction commit — once to flush the rollback journal and once to flush the
database file. During a publish with hundreds of catalog updates, this produces
hundreds of serialised `fsync` pairs, each stalling all SQLite writes until the
OS confirms the data has reached stable storage.

### Solution

The read-write branch of `Configure()` now applies:

```sql
PRAGMA synchronous=NORMAL;
```

`synchronous=NORMAL` omits the fsync on rollback-journal deletion (the fsync
that fires after every commit in FULL mode). Data is still written to the main
database file before the SQLite connection moves on, so
`ScheduleCatalogProcessing` can read the raw file with the spooler and upload
a correct, up-to-date catalog.

`journal_mode=WAL` was deliberately considered and rejected for this use case:
in WAL mode, committed pages live in a `<db>-wal` sidecar file until a
checkpoint is performed. The spooler reads only the main database file at the
OS level, so uploading before an explicit `PRAGMA wal_checkpoint` would silently
upload a stale (pre-commit) catalog — a correctness bug. `synchronous=NORMAL`
with the default DELETE journal gives the same fsync reduction without that
ordering hazard.

Read-only database opens (CVMFS clients, stratum-1 mirrors, catalog readers)
are unaffected; their `Configure()` branch is unchanged.

---

## Change 7 — Sequential read-ahead hint for ingestion source files

**Files:** `cvmfs/util/platform_linux.h`, `cvmfs/util/platform_osx.h`,
`cvmfs/ingestion/ingestion_source.h`

### Problem

`FileIngestionSource::Open()` opens files with `O_RDONLY` and begins reading
them in sequential blocks, but does not tell the kernel about the access
pattern. The kernel's heuristic read-ahead starts conservatively and ramps up
slowly, causing I/O stalls for the first several blocks of large files and
under-utilising the available I/O bandwidth when many read workers are active.

### Solution

A platform-abstracted `platform_fadvise_sequential(int fd)` function is added:

- Linux: `posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL)` — doubles the
  kernel read-ahead window and marks pages for discard after use.
- macOS: no-op (no `posix_fadvise` equivalent with equivalent semantics).

The call is placed immediately after the successful `open()` in
`FileIngestionSource::Open()`. `Close()` already calls
`platform_invalidate_kcache` (`POSIX_FADV_DONTNEED`) to release pages.

---

## Change 8 — Extended attribute pre-fetch moves off the pipeline thread

**Files:** `cvmfs/sync_item.h`, `cvmfs/sync_item.cc`,
`cvmfs/sync_mediator.cc`

### Problem

When `--include-xattrs` is active, `SyncMediator::PublishFilesCallback()`
calls `XattrList::CreateFromFile()` on the union filesystem path for every
completed file. This callback runs on the ingestion pipeline's observer thread
(fired from `TaskRegister` via `NotifyListeners`). Because `TaskRegister` has a
single worker, this serialises all `lgetxattr` syscalls — and therefore all
`catalog_manager_->AddFile()` catalog insertions — through a single thread,
regardless of how many files the compress/hash/write stages have processed in
parallel.

### Solution

`SyncItem` gains a `XattrList *cached_xattrs_` private field (null-initialised,
freed in the destructor) and ownership-transfer accessors `SetCachedXattrs()`
and `TakeCachedXattrs()`.

In `SyncMediator::AddFile()`, immediately before inserting the entry into
`file_queue_` (which happens on the traversal thread), xattrs are read and
stored on the `SyncItem`:

```cpp
if (params_->include_xattrs) {
  entry->SetCachedXattrs(XattrList::CreateFromFile(entry->GetUnionPath()));
}
```

In `PublishFilesCallback()`, the `XattrList::CreateFromFile()` call is replaced
with `item.TakeCachedXattrs()`. If the returned pointer is non-null it is used
directly and then `delete`-d; otherwise `default_xattrs_` is used as before.
The `TaskRegister` thread no longer performs any filesystem I/O.

---

## Building

```bash
pip install cmake --break-system-packages          # cmake >= 3.24
apt-get install -y libfuse3-dev libcap-dev libacl1-dev uuid-dev python3-dev

cmake -B build -S . \
  -DBUILD_UNITTESTS=ON \
  -DBUILD_UBENCHMARKS=ON \
  -DBUILTIN_EXTERNALS=ON

cmake --build build -j$(nproc)
```

## Testing

```bash
# Unit tests (focused on changed components)
./build/cvmfs_unittests \
  --gtest_filter='T_Compression*:T_Compressor*:T_CatalogSql*:T_CatalogMgrRw*:T_Shash*' \
  --gtest_print_time=1

# Micro-benchmarks for compression and hash throughput
./build/cvmfs_ubenchmarks \
  --benchmark_filter='BM_Compression|BM_Hash' \
  --benchmark_repetitions=5

# Full unit suite (skipping slow integration tests)
./build/cvmfs_unittests --gtest_filter=-*Slow
```
