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

Profiling and code inspection identified eleven independent bottlenecks. Each is
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

The `num_upload_tasks` field is initialised at construction time using
`GetNumberOfCpuCores()`:

```cpp
num_upload_tasks = std::min(16U, std::max(4U, GetNumberOfCpuCores() / 2))
```

This gives 4 upload tasks on a 4-core machine, scales linearly to 16 on a
32-core machine, and caps at 16 to avoid overwhelming backends with too many
concurrent connections.

`SyncParameters::num_upload_tasks` is changed from `1` to `0` (sentinel meaning
"inherit from SpoolerDefinition"). The override in `swissknife_sync.cc` is
made conditional: `spooler_definition.num_upload_tasks` is only overwritten
when the operator explicitly passes the `-0` flag, preserving the CPU-scaled
default in all other cases.

---

## Change 6 — Reduced fsync overhead and larger SQLite page cache for publish catalogs

**Files:** `cvmfs/sql_impl.h`

### Problem

`Database<DerivedT>::Configure()` applies pragmas only for read-only opens. For
read-write opens (publish-path writable catalogs) no pragmas are set, so SQLite
uses its defaults: `synchronous=FULL` (an `fsync()` after every transaction
commit), a 128-page buffer pool (≈ 512 KB with 4 KB pages), and no
memory-mapped I/O. During a publish with hundreds of catalog updates this
produces hundreds of serialised `fsync` pairs and repeated physical page reads
for hot catalog rows.

### Solution

The read-write branch of `Configure()` now applies three pragmas:

```sql
PRAGMA synchronous=NORMAL;
PRAGMA cache_size=-65536;
PRAGMA mmap_size=134217728;
```

**`synchronous=NORMAL`** omits the fsync on rollback-journal deletion that
fires after every commit in FULL mode. Data is still written to the main
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

**`cache_size=-65536`** sets a 64 MB page buffer pool (the negative value is
interpreted as kibibytes by SQLite ≥ 3.7.10, which CVMFS's bundled SQLite
satisfies). This keeps hot catalog pages in the process address space across
the many `AddEntry` and `UpdateCounters` calls that touch the same rows
repeatedly during a publish session.

**`mmap_size=134217728`** allows SQLite to memory-map up to 128 MB of the
database file. For catalogs that fit within the map window, random page reads
become simple memory accesses with no `pread()` syscall overhead. SQLite
re-establishes the mapping correctly after a VACUUM rewrites the file.

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

## Change 9 — Hardlink group ID cached per catalog

**Files:** `cvmfs/catalog_rw.h`, `cvmfs/catalog_rw.cc`,
`cvmfs/catalog_mgr_rw.cc`

### Problem

`WritableCatalogManager::AddHardlinkGroup()` called `catalog->GetMaxLinkId()`
once per hardlink group to obtain the next available group ID. Each call
executes `SELECT MAX(hardlinks>>32)` against the catalog database. With N
hardlink groups all landing in the same catalog, this is N redundant queries
where one would suffice — the result only changes because we just incremented
it ourselves.

### Solution

`WritableCatalog` gains two new private fields:

```cpp
bool     link_id_ready_;   // false until first IssueHardlinkGroupId() call
uint32_t link_id_seq_;     // next group ID to hand out
```

A new public method `IssueHardlinkGroupId()` replaces the `GetMaxLinkId() + 1`
call at the use site:

```cpp
uint32_t WritableCatalog::IssueHardlinkGroupId() {
  if (!link_id_ready_) {
    link_id_seq_   = GetMaxLinkId() + 1;  // one DB query, first call only
    link_id_ready_ = true;
  }
  assert(link_id_seq_ > 0);
  return link_id_seq_++;                  // pure in-memory increment thereafter
}
```

The first call during a publish session executes exactly one
`SELECT MAX(hardlinks>>32)` query; every subsequent call for the same catalog
increments `link_id_seq_` in memory. For a publish with N hardlink groups in a
single catalog, the query count drops from N to 1.

`GetMaxLinkId()` is retained for use in `CopyToParent()`, where a live DB query
is required to compute the rebase offset when merging a nested catalog into its
parent.

---

## Change 10 — O(1) hardlink callback lookup via index map

**Files:** `cvmfs/sync_mediator.h`, `cvmfs/sync_mediator.cc`

### Problem

`SyncMediator::PublishHardlinksCallback()` received a spooler result and located
the matching `HardlinkGroup` in `hardlink_queue_` by scanning the entire vector
with a string comparison on each element's `master->GetUnionPath()`. With N
hardlink groups this scan is O(N) per callback, and there are N callbacks —
O(N²) string comparisons in total.

### Solution

`SyncMediator` gains a new private field:

```cpp
std::unordered_map<std::string, size_t> hardlink_index_;
```

When a group is enqueued in `PrepareHardlinks()`, its master path and queue
position are recorded together:

```cpp
hardlink_index_[i->second.master->GetUnionPath()] = hardlink_queue_.size();
hardlink_queue_.push_back(i->second);
```

In `PublishHardlinksCallback()`, the O(N) scan is replaced by a single map
lookup:

```cpp
const auto idx_it = hardlink_index_.find(result.local_path);
assert(idx_it != hardlink_index_.end());
// update hardlink_queue_[idx_it->second] directly
```

Total callback work drops from O(N²) to O(N). The index is fully populated
before any spooler callbacks can fire and is never written during the callback
phase, so no additional synchronisation is required.

---

## Change 11 — `VacuumDatabaseIfNecessary` releases lock before I/O

**Files:** `cvmfs/catalog_rw.cc`

### Problem

`WritableCatalog::VacuumDatabaseIfNecessary()` held `lock_` for the entire
function body, including the `db.Vacuum()` call. SQLite VACUUM rewrites the
entire catalog file and can take several seconds on a large catalog. While the
lock was held, any concurrent caller of `UpdateNestedCatalog()` — which also
acquires `lock_` — would stall for the full VACUUM duration. With parallel leaf
finalization (Change 2), multiple finalization threads run concurrently and can
encounter this contention on shared parent catalogs.

### Solution

`lock_` is scoped tightly to just the metric-reading section; the VACUUM I/O
proceeds without holding it:

```cpp
{
  const MutexLockGuard m(lock_);
  if (db.GetFreePageRatio() > kMaximalFreePageRatio) { ... }
  else if (db.GetRowIdWasteRatio() > kMaximalRowIdWasteRatio) { ... }
}  // lock_ released here

if (needs_defragmentation) {
  db.Vacuum();  // runs without holding lock_
}
```

This is safe because `VacuumDatabaseIfNecessary()` is called only from
`FinalizeCatalog()`, which is itself triggered only after a catalog's
`dirty_children` counter reaches zero. That counter reaching zero is the
ordering guarantee that all concurrent `UpdateNestedCatalog()` calls on that
catalog have already completed — no other thread will acquire `lock_` on it
while VACUUM is in progress.

---

## Performance Estimates

### Reference workload

| Parameter | Value |
|---|---|
| Repository size | 2 GB, 500,000 files |
| Files published (new / changed) | 100,000 |
| Average file size | ~4 KB (software distribution: headers, configs, scripts, small binaries) |
| New content volume | ~400 MB |
| Nested catalogs (dirty after publish) | ~50 |
| Hardlink groups | ~5,000 |
| Server | 8-core, NVMe-backed overlayFS, local filesystem CAS backend |

All timings are first-principles estimates based on typical syscall latencies
and SQLite throughput figures.  They are not measurements; actual numbers
depend heavily on hardware, kernel version, filesystem driver, and repository
structure.  A ±30% band around each figure is realistic.

### Baseline publish time: ~60 s

| Phase | Time | Dominant cost |
|---|---|---|
| Scratch-layer traversal | 6 s | 100 K lstat + readdir syscalls through overlayFS |
| Ingestion pipeline | 12 s | Per-file open/read/compress/hash overhead for 100 K small files |
| CAS upload (objects) | 8 s | 100 K small-file writes, single-threaded |
| Catalog SQL | 4 s | 100 K INSERTs + 50 × 24 counter UPDATEs; synchronous page fetches |
| Catalog VACUUM | 22 s | ~12 catalogs × ~1.8 s each, executed serially |
| Catalog upload + manifest | 8 s | ~50 catalog files (~250 MB) + signing round-trip |
| **Total** | **60 s** | |

VACUUM dominates because it rewrites the entire SQLite file for each affected
catalog; with 12 catalogs needing defragmentation the serial cost accumulates
to more than a third of total publish time.

### Per-change improvement estimates

| Change | Phase affected | Mechanism | Saved (s) | Notes |
|---|---|---|---|---|
| 1 — Parallel traversal | Traversal (6 s) | 8 worker threads replace serial DFS | **~4.5 s** | Scales with core count; larger gain on NFS (50 µs/lstat vs 2 µs) |
| 2 — Parallel leaf VACUUM | VACUUM (22 s) | Up to N threads, one per leaf catalog | **~19 s** | 12 catalogs run concurrently; residual limited by disk bandwidth |
| 3 — Larger block sizes | Ingestion (12 s) | 8× fewer tube dispatches for files > 16 KB | **~1 s** | 4 KB average size means ~30 % of files cross the old 16 KB threshold |
| 4 — Hash merged into Compress | Ingestion (12 s) | Eliminates one inter-thread tube dispatch per block | **~1.5 s** | Every file benefits; also enables zero-copy passthrough for pre-compressed content |
| 5 — Upload task parallelism | CAS upload (8 s) | 1 → 4 upload tasks; 100 K small writes parallelised | **~5 s** | Largest relative gain on gateway / S3 backends (network RTT dominates) |
| 6 — SQLite pragmas | Catalog SQL (4 s) | 64 MB page cache + mmap eliminate page re-reads; synchronous=NORMAL drops post-journal fsync | **~1.5 s** | Benefit grows with catalog size; cold-cache runs see larger gains |
| 7 — Sequential read-ahead | Ingestion (12 s) | Kernel doubles read-ahead window for files > 128 KB | **~0.5 s** | Marginal for 4 KB average; significant for repos with large binaries |
| 8 — Xattr pre-fetch | Ingestion (12 s) | Moves lgetxattr off single-threaded TaskRegister onto traversal workers | **0 s / ~2 s** | **Zero unless `--include-xattrs` is active** |
| 9 — Hardlink ID cache | Ingestion (12 s) | 5 K groups × 1 DB query → 50 queries (one per catalog) | **~1 s** | Proportional to hardlink group count |
| 10 — O(1) hardlink lookup | Ingestion (12 s) | O(N²) string scans → O(N) hash-map lookups | **~0.1 s** | Negligible at 5 K groups; matters at 100 K+ groups |
| 11 — Vacuum lock narrowing | VACUUM (22 s) | Releases lock_ before I/O so parent finalization is not blocked | **~2.5 s** | Synergistic: without this, parallel VACUUM (Change 2) stalls on parent catalog lock |

Changes 2 and 11 are synergistic: Change 11 removes the locking bottleneck
that would otherwise prevent Change 2 from achieving full thread-level
parallelism.  Their combined saving of ~21.5 s is attributed jointly.

### Combined estimate (all changes, `--include-xattrs` disabled)

| Phase | Baseline | After | Saved |
|---|---|---|---|
| Traversal | 6 s | 1.5 s | 4.5 s |
| Ingestion pipeline | 12 s | 8 s | 4 s |
| CAS upload | 8 s | 3 s | 5 s |
| Catalog SQL | 4 s | 2.5 s | 1.5 s |
| Catalog VACUUM | 22 s | 2.5 s | 19.5 s |
| Catalog upload + manifest | 8 s | 8 s | 0 s |
| **Total** | **60 s** | **~26 s** | **~34 s** |

**Overall speedup: ~2.3× (57 % reduction in publish time).**

With `--include-xattrs` active: additional ~2 s saved → ~2.4× speedup.

### Sensitivity to deployment conditions

| Condition | Effect on total saving |
|---|---|
| NFS-backed overlayFS (lstat ~50 µs) | Traversal baseline ~30 s; Change 1 saves ~25 s; total speedup grows to ~3–4× |
| Gateway or S3 backend (10 ms/object RTT) | CAS upload baseline ~1,000 s for 100 K objects at 1 task; Change 5 alone saves ~750 s |
| Mostly large files (avg > 1 MB) | Changes 3, 4, 7 together give 2–3× ingestion speedup instead of ~1.5× |
| Few or no hardlinks | Changes 9 & 10 contribute < 0.1 s; negligible |
| Few catalogs needing VACUUM | Changes 2 & 11 contribute proportionally less; floor ~1 s |
| 32-core server | Change 1 scales to ~12× traversal speedup; Change 2 parallelises all leaf VACUUMs simultaneously |

---

### End-to-end impact with cvmfs-bits

The pipeline changes above reduce the Stratum-0 publish time from ~60 s to
~26 s.  cvmfs-bits changes the scope of what is measured: the relevant figure
shifts from "time until Stratum-0 commits" to "time until every Stratum-1
mirror is consistent and all worker nodes can efficiently access new content".

#### Additional scenario parameters

| Parameter | Value |
|---|---|
| Stratum-1 mirrors | 5, geographically distributed |
| Stratum-0 → Stratum-1 bandwidth (datacenter) | 10 Gbps shared |
| Stratum-1 → client bandwidth | 1 Gbps |
| Concurrent worker nodes accessing new version | 100 |
| Files accessed per job (subset of new content) | ~10,000 files (~40 MB) |
| Content dedup rate (bloom filter, rolling release) | ~25 % (objects from prior version already on mirrors) |

#### Metric definitions

**Time to publish (T_pub):** wall time from invoking the publish command to the
new root catalog hash being signed and recorded on Stratum-0.  This is what the
pipeline changes above optimise.

**Time to consistency (T_con):** time from T_pub until all Stratum-1 mirrors
hold a complete, queryable copy of every new object and the updated catalogs.
Before cvmfs-bits, Stratum-1 replication is a passive pull triggered after
T_pub; with cvmfs-bits it is a proactive push that begins during the publish
pipeline and finishes at or before T_pub.

**Time to production (T_pro):** time from invoking publish until all 100 worker
nodes have successfully opened the new catalog and can read every required file
at full throughput.  This is the figure that determines how long an HPC slot
sits idle waiting for software.

#### Estimates

| Scenario | T_pub | T_con | T_pro | Notes |
|---|---|---|---|---|
| **Baseline** — no pipeline improvements, no cvmfs-bits | 60 s | 60 s + 90 s = **150 s** | 150 s + 30 s = **180 s** | Stratum-1 pulls 400 MB serially after commit; 100 workers cold-fetch simultaneously (thundering herd) |
| **Pipeline improvements only** — Changes 1–11, no cvmfs-bits | 26 s | 26 s + 90 s = **116 s** | 116 s + 30 s = **146 s** | Same replication lag; thundering herd unchanged |
| **Improvements + cvmfs-bits, no pre-warming** | 26 s | **~26 s** | 26 s + 30 s = **56 s** | Content pre-pushed to all 5 mirrors in parallel during publish; replication lag eliminated; cold-fetch thundering herd remains |
| **Improvements + cvmfs-bits + cache pre-warming** | **~29 s** | **~29 s** | **~31 s** | Pre-warming adds ~3 s (400 MB × 0.75 dedup → 300 MB pushed at 10 Gbps, pipelined with ingestion); clients read from warm Stratum-1 cache → thundering herd eliminated |

#### How the numbers are derived

**Stratum-1 replication lag without cvmfs-bits (~90 s):**
A Stratum-1 mirror receives a notify webhook on commit, then pulls the changed
root catalog, walks the delta, and fetches 400 MB of new objects.  At a
realistic pull rate of ~300 Mbps sustained (competing with client traffic on a
1 Gbps link): 400 MB / 37.5 MB/s ≈ 11 s per mirror for data alone, plus 5–10 s
catalog walk and HTTP overhead per object → easily 60–120 s wall time,
serialised through the Stratum-0 HTTP server.  Five mirrors pulling
simultaneously saturate Stratum-0 uplink and push this to 90 s median.

**Thundering herd cold-fetch (~30 s):**
100 worker nodes simultaneously open the new catalog version.  Each node reads
~10,000 files × 4 KB = 40 MB.  With a cold Stratum-1 cache, these 100 × 40 MB
= 4 GB of requests arrive within seconds.  At 1 Gbps Stratum-1 output bandwidth
shared across 100 clients: each client waits an average of 4 GB / 125 MB/s / 10
concurrent = 3.2 s for data, but with per-file RTT overhead (~0.5 ms × 10,000
files) and OS page-fault stalls the practical wait before a job's first real
work is done is 20–40 s.  Median ~30 s.

**cvmfs-bits parallel push (overlaps ingestion, net +0 s for T_pub):**
cvmfs-bits streams compressed objects to all 5 Stratum-1 mirrors concurrently
as soon as the ingestion pipeline produces them.  At 10 Gbps datacenter
bandwidth, 300 MB (after 25 % bloom-filter dedup) takes 300 MB / 1,250 MB/s
= 0.24 s per mirror; with 5 mirrors in parallel and pipelining the push
overlaps almost entirely with the 8 s ingestion phase.  Net addition to T_pub:
~0 s.  Catalogs (250 MB) are pushed immediately after finalization, also within
the existing T_pub budget.

**Cache pre-warming (+3 s, eliminates thundering herd):**
Pre-warming pushes the 100 most-accessed entry points (root catalog, top-level
directory listings, the ~10,000 files that jobs access on startup) to each
Stratum-1 node's memory cache before the gateway lease is committed.  This adds
~3 s to T_pub (300 MB of targeted pre-warm data per mirror at 10 Gbps,
sequential after catalog finalization).  After commit, client cold-fetches hit
an already-warm cache: per-file latency drops to sub-millisecond, and 100
simultaneous workers consume from cache bandwidth rather than object-store
bandwidth.  T_pro ≈ T_pub + ~2 s (catalog open + first file access).

#### Summary

```
Scenario                                     T_pub   T_con   T_pro
─────────────────────────────────────────────────────────────────────
Baseline (no improvements, no cvmfs-bits)     60 s   150 s   180 s
Pipeline improvements only                    26 s   116 s   146 s
Improvements + cvmfs-bits (no pre-warming)    26 s    26 s    56 s
Improvements + cvmfs-bits + pre-warming       29 s    29 s    31 s
─────────────────────────────────────────────────────────────────────
Speedup vs baseline (T_pro)                   2.3×    5.2×    5.8×
```

The pipeline changes alone yield a **2.3× reduction in publish time** but leave
the end-to-end worker wait largely dominated by Stratum-1 replication lag and
cold-fetch overhead.  Adding cvmfs-bits eliminates the replication lag and,
with pre-warming, the thundering herd as well — pushing the overall
**T_pro speedup to ~5.8×** relative to the unmodified baseline.

---

## Gateway Analysis

The CVMFS gateway (`cvmfs/gateway/`) sits on the T_pub critical path for every
publish that goes through a lease: a publisher acquires a lease, uploads
payload, and commits — all via gateway RPC calls.  For the reference workload
(2 GB release, 100 K new files, single repository, single active publisher) the
gateway currently adds **~300 ms – 2 s** of wall-clock overhead to T_pub.  This
section identifies the bottlenecks discovered by reading the gateway source and
estimates the saving available from each.

### Gateway role in T_pub

```
Publisher                     Gateway                    Storage / cvmfs_receiver
────────────────────────────────────────────────────────────────────────────────
AcquireLease(repo, path) ──►  leaseMutex.Lock()
                              DB: scan for overlapping/expired leases
                              DB: INSERT new lease
                              leaseMutex.Unlock()         ◄── ~50 ms
◄── lease token ─────────────
[publisher uploads, ingests, finalizes catalog — ~26 s with pipeline changes]
SubmitPayload(token, data) ──► forward to worker pool
                               worker spawns cvmfs_receiver process
                               receiver.SubmitPayload(data)  ◄── ~200 ms–1.5 s
◄── ack ─────────────────────
CommitLease(token) ──────────► leaseMutex.Lock()
                               worker spawns new cvmfs_receiver process
                               receiver.Commit(...)         ◄── ~200 ms–500 ms
                               leaseMutex.Unlock()
◄── done ────────────────────
```

For a single-repo, single-publisher flow the gateway is not a bottleneck in
the 26 s headline number — it contributes < 5 % of T_pub.  However, several
structural issues become significant at scale or under concurrent publishing.

### Bottleneck catalogue

#### CRITICAL — Global lease mutex held through receiver commit

**File:** `gateway/internal/gateway/backend/lease_service.go`, line 12

```go
var leaseMutex sync.Mutex   // one lock for the entire gateway process
```

`CommitLease()` acquires `leaseMutex` before spawning the `cvmfs_receiver`
child and holds it for the full duration of the receiver's `Commit()` call
(200 ms – 5 s of I/O).  During that window every other `NewLease`,
`CommitLease`, or `CancelLease` call on **any** repository blocks.

**Impact (multi-repo):** With 10 repositories each publishing a 1 GB release
simultaneously, the effective commit throughput is serialised to
`1 / (avg_commit_time)` ≈ 1 commit every ~1 s instead of 10 in parallel.
Total wall time for 10 concurrent publishes: **~10 s** instead of ~1 s.

**Fix:** Replace the single global mutex with a per-repository (or
per-lease-path) `sync.RWMutex`.  `NewLease` and `CommitLease` for different
repositories then proceed concurrently.  The per-repo lock still serialises
overlapping paths within one repo, which is the correct invariant.

**Estimated saving (single-repo):** ~0 ms (lock contention only appears with
concurrency).  **Estimated saving (10 concurrent repos):** ~9 s wall time
recovered.

#### HIGH — Per-task `cvmfs_receiver` process spawned for every operation

**File:** `gateway/internal/gateway/receiver/pool.go`, lines 149–161

```go
receiver, err := NewReceiver(task.Context(), pool.workerExec, pool.mock, pool.smgr)
defer func() { receiver.Quit() }()
// ... handle one payload or commit, then throw the process away
```

For every `SubmitPayload` and `CommitLease` RPC the worker function forks a
new `cvmfs_receiver` child, initialises it (open DB connections, load keys,
etc.), does one unit of work, then sends `Quit()`.  Process creation on Linux
costs ~10–50 ms; `cvmfs_receiver` initialisation (SQLite open, signature key
load) adds another ~50 ms.  For a typical publish with 5 payload chunks +
1 commit that is **~300 ms – 600 ms** of pure fork overhead.

**Fix:** Maintain a pool of persistent `cvmfs_receiver` processes — one (or a
small fixed number) per repository.  Workers send work to the resident process
via the existing pipe protocol and reuse it for the next task.  The
`receiver.Quit()` call moves from per-task to gateway shutdown.

**Estimated saving (single-repo):** ~300–600 ms off T_pub — equivalent to
roughly half the current gateway overhead for the reference workload.

#### MEDIUM — Synchronous `SubmitPayload` blocks publisher goroutine

**File:** `gateway/internal/gateway/receiver/pool.go`, `SubmitPayload` method

The publisher sends a payload chunk and then **blocks** waiting for the
receiver's acknowledgement before sending the next chunk.  This prevents
pipelining: even if the receiver could accept chunk N+1 while writing chunk N,
the publisher waits idle.

**Fix:** Allow the publisher to have K chunks in-flight (e.g., a semaphore of
size 4).  The worker pool replies asynchronously; the publisher advances as
soon as a slot is free.  Alternatively, stream the payload as a single
chunked-transfer HTTP body so the gateway accumulates and forwards in a single
pass.

**Estimated saving:** ~100–400 ms for a 2 GB payload split into 5 chunks over
a 10 Gbps link.

#### MEDIUM — Expired-lease scan on every `NewLease`

**File:** `gateway/internal/gateway/backend/lease_service.go`, `NewLease`

Every `NewLease` call runs `DeleteAllExpiredLeases()` (a full table scan) while
holding `leaseMutex`.  For a small DB with tens of leases this is negligible.
Under load (many short-lived leases) the scan serialises all lease acquisitions.

**Fix:** Run expiry cleanup in a background goroutine on a 30 s ticker, not on
every `NewLease` hot path.

**Estimated saving (single-repo, quiescent DB):** < 5 ms.  **Estimated saving
(busy gateway with hundreds of leases):** 20–50 ms per `NewLease`.

#### LOW — DB opened per request in some code paths

Several helper functions re-open the BoltDB handle instead of reusing a
long-lived connection.  BoltDB is file-locked; repeated open/close cycles add
~5–10 ms and prevent write batching.

**Fix:** Share a single opened `*bolt.DB` for the gateway's lifetime via the
existing `GatewayOptions` / `BackendServices` context.

**Estimated saving:** ~5–20 ms per publish.

### Single-repo T_pub impact

For the reference workload (single repository, single concurrent publisher,
pipeline improvements already applied — T_pub baseline 26 s):

| Gateway change | Saving | New T_pub |
|---|---|---|
| None (status quo) | — | 26.0 s |
| Persistent receiver pool | −450 ms | 25.5 s |
| Async payload submission | −200 ms | 25.3 s |
| Background lease expiry | −10 ms | 25.3 s |
| Shared DB handle | −10 ms | 25.3 s |
| **All gateway changes** | **~670 ms** | **~25.3 s** |

The gateway is not the bottleneck for single-repo publishing — the ~670 ms
saving is real but small compared to the 34 s saved by the pipeline changes.
The architectural significance is at scale: **the global mutex fix enables
fully concurrent multi-repo publishing**, which is otherwise serialised.

### Multi-repo concurrent publishing

With per-repo locks and a persistent receiver pool, 10 repositories can publish
simultaneously.  Each publisher's T_pub is independent (≈ 25.3 s).  Gateway
wall time for 10 concurrent publishes:

```
Status quo (global mutex):    ~10 × 1 s commit ≈ 10 s serialised gate time
With per-repo lock:           ~0.3 s per repo (parallel) — gate overhead < 0.5 s
```

The throughput improvement for concurrent multi-repo scenarios is therefore
**~20× reduction in gateway-attributed wall time** (from ~10 s down to ~0.5 s
for 10 parallel publishes).

### Summary

The gateway is a minor contributor to T_pub in single-repo publishing
(~300 ms–2 s out of 60 s baseline / 26 s improved) but has two structural
issues that limit scalability:

1. **Global `leaseMutex`** serialises all repositories — fix yields ~20×
   multi-repo throughput improvement.
2. **Per-task receiver spawn** wastes ~300–600 ms per publish — fix by
   maintaining a persistent receiver process pool.

Both changes have been implemented (see Gateway Changes below).

---

## Gateway Changes

### Change G1 — Persistent receiver process pool

**File:** `gateway/internal/gateway/receiver/pool.go`

#### Problem

The `worker` function in the original pool spawned a new `cvmfs_receiver`
child process for every task — one for each payload submission and one for each
commit.  Process creation on Linux costs ~10–50 ms; `cvmfs_receiver`
initialisation (SQLite open, key load) adds another ~50 ms.  For a typical
publish with a few payload chunks plus one commit, that is ~300–600 ms of pure
fork overhead per publish.

#### Solution

Each worker goroutine now owns a **persistent** `Receiver` process kept alive
across tasks.  A new process is spawned lazily on the first task and only
replaced when the previous one crashes.

```go
// Per-worker persistent process; nil means not yet started or dead.
var recv Receiver

ensureReceiver := func(ctx context.Context) error {
    if recv != nil {
        return nil
    }
    recv, err = NewReceiver(ctx, pool.workerExec, pool.mock, pool.smgr)
    ...
}

discardReceiver := func(ctx context.Context) {
    recv.Quit()   // best-effort; logs on error
    recv = nil
}
```

**Crash detection** distinguishes application-level errors (type `Error`, a
string alias returned by `parseReceiverReply`) from I/O errors caused by a
dead process.  Only the latter trigger `discardReceiver`:

```go
var appErr Error
if !errors.As(result, &appErr) {
    // I/O error: receiver process died — replace it next task.
    crashed = true
}
```

`testCrashTask` always sets `crashed = true` because `TestCrash` terminates
the process regardless of whether an error is returned.

On pool shutdown (`Stop()` closes the task channel), the `defer` at the top of
each worker goroutine cleanly calls `recv.Quit()`.

**Estimated saving:** ~300–600 ms per publish (eliminated for steady-state
traffic where the process is already running).

---

### Change G2 — Per-repository lease locks

**Files:** `gateway/internal/gateway/backend/lease_service.go`,
`gateway/internal/gateway/backend/locks.go`

#### Problem

A single package-level `var leaseMutex sync.Mutex` serialised every lease
operation — `NewLease`, `CommitLease`, `CancelLease`, `CancelLeases`,
`GetLease`, `GetLeases` — across **all** repositories.  Because
`CommitLease` holds this mutex for the full duration of the
`cvmfs_receiver` commit (200 ms – 5 s of I/O), any concurrent lease
operation on any repository blocks for that entire window.  With ten
repositories publishing simultaneously this adds ~10 s of serialised gate
time.

#### Solution

The global mutex is replaced by a package-level `NamedLocks` (the existing
per-name mutex map already present in `locks.go`):

```go
// Per-repository mutual exclusion for lease operations.
var leaseNamedLocks NamedLocks
```

**Lock scope per operation:**

| Operation | Lock acquired | Notes |
|---|---|---|
| `NewLease` | `leaseNamedLocks[repo]` | wraps entire check-and-insert |
| `CancelLeases` | `leaseNamedLocks[repo]` | repo extracted from path argument |
| `CancelLease` | `leaseNamedLocks[repo]` | repo looked up via pre-read |
| `CommitLease` | `leaseNamedLocks[repo]` → `DB.Locks[repo]` | nested; see below |
| `GetLease` | none | read-only; SQL tx isolation sufficient |
| `GetLeases` | none | read-only; SQL tx isolation sufficient |

**Two-phase locking for token-based operations.**  `CancelLease` and
`CommitLease` receive only a token, not a repository name.  A brief
read-only DB lookup (`repoForToken`) runs outside any lock to obtain the
repository name; the lease is then re-validated inside the per-repo lock to
close the TOCTOU window:

```go
// Pre-read (no lock): find the repository for locking.
repo, err := s.repoForToken(ctx, token)

// Critical section: re-validate and commit inside per-repo lock.
leaseNamedLocks.WithLock(repo, func() error {
    lease, err := FindLeaseByToken(ctx, tx, token) // re-check
    if lease == nil || lease.Expiration.Before(time.Now()) {
        return InvalidLeaseError{}
    }
    // DB.WithLock serialises commits and GC for the same repo.
    s.DB.WithLock(ctx, lease.Repository, func() error {
        finalRev, err = s.Pool.CommitLease(...)
        return err
    })
    DeleteLeaseByToken(...)
    return tx.Commit()
})
```

**Lock ordering** is always `leaseNamedLocks[repo]` → `DB.Locks[repo]` (or
standalone `DB.Locks[repo]` for GC).  No code path acquires these in the
reverse order, so deadlock is impossible.

**Estimated saving (single-repo):** < 5 ms (contention only appears with
concurrent publishers).  **Estimated saving (10 concurrent repos):**
~9 s wall time recovered — ten publishes now proceed in parallel instead
of being serialised through the commit I/O of each predecessor.

---

### Bug fixes found during review

Three bugs were identified during post-implementation review and corrected in
the same commit.

**Bug A — `CancelLease` regressed on expired leases (`lease_service.go`)**

`repoForToken` returned `InvalidLeaseError` for any lease whose
`Expiration.Before(time.Now())` was true.  `CancelLease` used this helper for
its pre-read, so it could no longer cancel an expired-but-still-present lease.
The original code checked only `lease == nil`; cancelling an expired lease is
valid (and is the mechanism by which stale locks are cleared externally).

Fix: added `repoForTokenAny`, which returns the repository for any existing
token regardless of expiry.  `CancelLease` now uses `repoForTokenAny`; the
stricter `repoForToken` is kept for `CommitLease` (committing an expired lease
must remain an error).  The inside-lock re-check in `CancelLease` already only
tested `lease == nil`, so no change was needed there.

**Bug B — SQLite BUSY errors under concurrent publishers (`db.go`)**

The global `leaseMutex` previously serialised every SQLite write operation,
making `SQLITE_BUSY` impossible in practice.  Per-repo locks allow concurrent
write transactions for different repositories; without a busy timeout the SQLite
driver returns `SQLITE_BUSY` immediately on contention instead of retrying.

Fix: appended `&_busy_timeout=5000` to the connection string.  SQLite will now
retry write lock acquisition for up to 5 s before returning an error, covering
the typical commit window of 200 ms – 2 s.

**Bug C — Inconsistent reply-channel close (`pool.go`)**

The `ensureReceiver`-failure path and the `default` (unknown task type) path
sent to the reply channel but did not close it, while the normal task path did.
For buffered channels with a single receiver this is not a correctness bug, but
the inconsistency could become one if callers are ever changed to range over or
select on the channel.

Fix: `close(task.Reply())` added to all three send-then-return paths.

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

New test cases added alongside this branch cover the four key changes that
are hardest to verify by inspection alone:

| Test | File | Change covered |
|---|---|---|
| `T_CatalogMgrRw/IssueHardlinkGroupIdSequence` | `t_catalog_mgr_rw.cc` | Change 9 — cache initialises from `GetMaxLinkId()` on first call; subsequent calls return 1, 2, 3 without a DB round-trip. |
| `T_CatalogMgrRw/HardlinkGroupsGetDistinctIds` | `t_catalog_mgr_rw.cc` | Changes 9 & 10 — two `AddHardlinkGroup` calls through the manager assign distinct, sequential, non-zero IDs; intra-group members share an ID. |
| `T_CatalogSql/WritableDbPragmasApplied` | `t_catalog_sql.cc` | Change 6 — a read-write `CatalogDatabase::Open` sets `cache_size=-65536` and `mmap_size=134217728`; a read-only open leaves the defaults intact. |
| `T_CatalogMgrRw/VacuumDatabaseNoDeadlock` | `t_catalog_mgr_rw.cc` | Change 11 — 50 add/remove cycles followed by `Commit()` completes without deadlock and leaves the catalog queryable. |
