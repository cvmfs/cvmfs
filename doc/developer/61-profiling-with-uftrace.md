# Function-level Profiling with uftrace

[uftrace](https://github.com/namhyung/uftrace) records a complete function call
tree of a running program. It is the most convenient way to answer questions
like *"which functions are actually executed during `cvmfs_server publish`, and
in what order?"* — for both performance work and for understanding an unfamiliar
code path.

uftrace has two modes:

- **Dynamic tracing** (`-P <pattern>`), which patches functions at runtime and
  needs no special build. This does **not** work reliably on the CernVM-FS
  binaries (see [Caveats](#caveats)).
- **Compiled instrumentation** (`-pg`/`libmcount`), where the compiler inserts an
  `mcount` call at every function entry. This is robust and is what we
  recommend. The `ENABLE_UFTRACE` CMake option turns it on.

> **_NOTE_** &nbsp;
> This page uses `cvmfs_server publish` (which runs `cvmfs_swissknife sync`) as
> the running example because it produces a rich call tree. The same recipe
> works for any CernVM-FS binary — `cvmfs2`, `cvmfs_swissknife`, the unit
> tests, etc.


## 0. uftrace version

Use uftrace **v0.20 or newer**. Older releases do not reliably follow the
forked and daemonized watchdog process that every CernVM-FS binary spawns, and
`uftrace record` then hangs at exit waiting for it. There is no distribution
package for EL10, so build from source if needed:

```bash
git clone https://github.com/namhyung/uftrace.git && cd uftrace
./configure --prefix=/usr/local && make -j$(nproc) && sudo make install
```

It's not possible to work around the hang with the `finish` trigger
(`-T <func>@finish`): it fires on function *entry*, so the trace ends before any
work is recorded.


## 1. Build with instrumentation

Configure with `-DENABLE_UFTRACE=ON`. This adds `-pg` to the C and C++ flags;
the base flags already provide `-g` and `-fno-omit-frame-pointer`, which uftrace
also relies on.

```bash
mkdir build-uftrace && cd build-uftrace
cmake -DENABLE_UFTRACE=ON -DBUILD_SERVER=ON ../
make -j$(nproc)
sudo make install     # so cvmfs_server picks up the instrumented binaries
```

Verify the binary is instrumented — it should contain `mcount` calls:

```bash
nm build-uftrace/cvmfs/cvmfs_swissknife | grep -i mcount
```

> **_NOTE_** &nbsp;
> `-pg` slows the binary down and every instrumented function shows up in the
> trace, so use a dedicated build directory and do not ship these binaries.
> Because the default optimization level is `-Os`, small functions may still be
> inlined and therefore invisible to the trace. If you need to see them,
> additionally configure with a lower optimization / `-fno-inline` in
> `CMAKE_CXX_FLAGS`.


## 2. Caveats specific to CernVM-FS

The server publish path stresses several things that trip uftrace up. Knowing
them up front saves a lot of time:

| Problem | Symptom | Fix |
|--|--|--|
| **Old uftrace and the watchdog** | `uftrace record` never finalizes / hangs on exit | Use uftrace v0.20 or newer. Older versions lose track of the forked, daemonized watchdog process; with v0.20 it is traced and finalized like any other task |
| **debuginfod fetch on an offline/firewalled host** | `uftrace record` hangs at the very end (in symbolization); `strace` shows a `connect()` to `debuginfod.*:443` stuck in `EINPROGRESS`/`POLLOUT` | `export DEBUGINFOD_URLS=` (empty). Ubuntu sets this by default via `/etc/profile.d/debuginfod.sh`; uftrace links `libdw`, whose debuginfod client tries to download debuginfo over HTTPS |
| **`perf`/hardware events** | record blocks or lacks permission | Add `--no-event` |
| **uftrace's return hook corrupts SQLite** | `sqlite::Sql::LazyInit(): Assertion 'success' failed`, `SIGABRT` | Add `--estimate-return` |
| **Missing capabilities** | `Initialization of the synchronisation engine failed` (exit 4) | Run the traced command under `sudo` (overlayfs / trusted xattrs need `CAP_SYS_ADMIN`) |
| **autofs on `/cvmfs`** | `Autofs on /cvmfs has to be disabled` | `sudo systemctl stop autofs` (restore with `start` afterwards) |
| **Dynamic tracing (`-P`)** | timeouts, trampoline corruption on C++ exceptions, deadlocks under the thread pools | Use compiled `-pg` instrumentation (this page), not `-P` |


## 3. Record a trace

The knobs above combine into a single `uftrace record` invocation. Because
`cvmfs_server` is a shell wrapper, point uftrace directly at the C++ binary via
its command line, or wrap the whole `cvmfs_server` call.

Recording `cvmfs_swissknife sync` (the core of `publish`) directly:

```bash
sudo systemctl stop autofs        # only if autofs manages /cvmfs

sudo DEBUGINFOD_URLS= \
  uftrace record \
    --no-event \
    --estimate-return \
    -d ./uftrace.data \
    /path/to/build-uftrace/cvmfs/cvmfs_swissknife sync \
      <the sync arguments cvmfs_server would pass>

sudo systemctl start autofs       # restore
```

(`DEBUGINFOD_URLS=` disables the elfutils debuginfod download that otherwise
hangs record's finalize step on an offline/firewalled host — see the caveats
table.)

To discover the exact `sync` arguments, run the publish once with tracing in the
wrapper. `cvmfs_server` builds the command in `cvmfs_server_publish()`; the
simplest approach is to prefix the whole publish and let uftrace follow the
`cvmfs_swissknife` child:

```bash
sudo uftrace record --no-event --estimate-return -d ./uftrace.data \
    cvmfs_server publish <repo.name>
```

A successful run writes per-thread `*.dat` files under `uftrace.data/`.


## 4. Inspect the trace

```bash
# Full replay (the call tree with timings)
uftrace replay -d ./uftrace.data | less

# Flat report, sorted by number of calls
uftrace report -s call -d ./uftrace.data

# Call graph rooted at a single function, limited depth
uftrace graph -d ./uftrace.data -D 4 swissknife::CommandSync::Main
```

`uftrace graph <function>` is especially useful for reading one subsystem in
isolation (e.g. `publish::SyncMediator::AddFile` or
`catalog::WritableCatalogManager::Commit`).


## 5. Generate a flame graph

uftrace exports FlameGraph-compatible folded stacks, that can be fed straight
into Brendan Gregg's [FlameGraph](https://github.com/brendangregg/FlameGraph)
`flamegraph.pl`:

```bash
# Fold stacks (weighted by self time: 1 unit == 1 us of self time)
uftrace dump --format=flame-graph --sample-time=1us -d ./uftrace.data > publish.folded

# Render an interactive SVG
./flamegraph.pl \
    --title "cvmfs_server publish (cvmfs_swissknife sync) — self time" \
    --countname us --width 1600 --hash \
    publish.folded > publish-flamegraph.svg
```

Drop `--sample-time` to weight the graph by *call count* instead of time. Each
thread becomes its own flame-graph root, so the main publish thread and the
upload/ingestion worker threads appear side by side.

> **_NOTE_** &nbsp;
> Width is *self time*, so most of the worker threads' area is
> `pthread_cond_wait` / `Tube::PopFront` — which are the workers idling in the queue. The actual
> publish will show up under `main → swissknife::CommandSync::Main`. Use the
> flame graph's search (Ctrl-F) to jump to a function of interest.

