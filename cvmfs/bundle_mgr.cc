/**
 * This file is part of the CernVM File System.
 */

#include "bundle_mgr.h"

#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "cache.h"
#include "catalog_mgr_client.h"
#include "fetch.h"
#include "file_chunk.h"
#include "json_document.h"
#include "mountpoint.h"
#include "options.h"
#include "shortstring.h"
#include "util/posix.h"

namespace {
constexpr size_t kDefaultBundlePoolSize = 8;

// Read the .cvmfsbundle-<basename> file via the cvmfs cache
BundleFileMgr *LoadBundleFromCvmfs(MountPoint *mp,
                                   const PathString &bundle_file_path) {
  catalog::DirectoryEntry dirent;
  if (!mp->catalog_mgr()->LookupPath(bundle_file_path, catalog::kLookupDefault,
                                     &dirent)) {
    LogCvmfs(kLogCvmfs, kLogDebug, "BUNDLE-LOAD: LookupPath failed for %s",
             bundle_file_path.ToString().c_str());
    return nullptr;
  }
  cvmfs::Fetcher *fetcher = mp->fetcher();
  if (fetcher == nullptr) {
    LogCvmfs(kLogCvmfs, kLogDebug, "BUNDLE-LOAD: fetcher is null");
    return nullptr;
  }

  CacheManager::Label label;
  label.path = bundle_file_path.ToString();
  label.size = dirent.size();
  label.zip_algorithm = dirent.compression_algorithm();
  const int fd = fetcher->Fetch(
      CacheManager::LabeledObject(dirent.checksum(), label));
  if (fd < 0) {
    LogCvmfs(kLogCvmfs, kLogDebug, "BUNDLE-LOAD: Fetch returned fd=%d", fd);
    return nullptr;
  }

  CacheManager *cache_mgr = mp->file_system()->cache_mgr();
  std::string content;
  content.resize(static_cast<size_t>(dirent.size()));
  const int64_t n = cache_mgr->Pread(fd, &content[0], content.size(), 0);
  cache_mgr->Close(fd);
  if (n < 0 || static_cast<size_t>(n) != content.size()) {
    LogCvmfs(kLogCvmfs, kLogDebug, "BUNDLE-LOAD: Pread returned %ld want %zu",
             static_cast<long>(n), content.size());
    return nullptr;
  }

  // The bundle file may start with a "#%CVMFS_BUNDLE version=..." header
  // line (per file_bundle.h); strip any leading lines beginning with '#'
  // before handing off to the strict JSON parser.
  size_t json_start = 0;
  while (json_start < content.size() && content[json_start] == '#') {
    const size_t nl = content.find('\n', json_start);
    if (nl == std::string::npos) {
      json_start = content.size();
      break;
    }
    json_start = nl + 1;
  }
  const std::string json_text = (json_start == 0) ? content
                                                  : content.substr(json_start);

  JsonDocument *doc = JsonDocument::Create(json_text);
  if (doc == nullptr) {
    LogCvmfs(kLogCvmfs, kLogDebug,
             "BUNDLE-LOAD: JsonDocument::Create failed (size=%zu)",
             json_text.size());
    return nullptr;
  }
  LogCvmfs(kLogCvmfs, kLogDebug, "BUNDLE-LOAD: loaded bundle %s (%zu bytes)",
           bundle_file_path.ToString().c_str(), content.size());
  return new BundleFileMgr(doc);
}
}  // namespace

BundleMgr::BundleMgr(MountPoint *mp)
    : mount_point_(mp)
    , fetcher_threads_()
    , pool_size_(kDefaultBundlePoolSize) {
  atomic_init32(&terminating_);
  pthread_mutex_init(&worker_read_mutex_, nullptr);

  // Pool size override via CVMFS_BUNDLE_POOL_SIZE
  if (mount_point_ != nullptr && mount_point_->file_system() != nullptr
      && mount_point_->file_system()->options_mgr() != nullptr) {
    std::string opt;
    if (mount_point_->file_system()->options_mgr()->GetValue(
            "CVMFS_BUNDLE_POOL_SIZE", &opt)) {
      char *end = nullptr;
      const unsigned long n = std::strtoul(opt.c_str(), &end, 10);
      if (end != opt.c_str() && n >= 1) {
        pool_size_ = static_cast<size_t>(n);
      }
    }
  }

  // The queues are created here so that ScheduleTrigger() can already
  // enqueue before Spawn(); pipe file descriptors, unlike threads, survive
  // the fuse client's daemonization fork.
  MakePipe(pipe_bm_);
  MakePipe(pipe_triggers_);

  // Non-blocking writes so TrySendPath/ScheduleTrigger can drop when a
  // queue is full. Per pipe(7), writes <= PIPE_BUF are atomic on
  // non-blocking pipes: they either fully succeed or fail with EAGAIN.
  int flags = fcntl(pipe_bm_[1], F_GETFL);
  fcntl(pipe_bm_[1], F_SETFL, flags | O_NONBLOCK);
  flags = fcntl(pipe_triggers_[1], F_GETFL);
  fcntl(pipe_triggers_[1], F_SETFL, flags | O_NONBLOCK);
}

void BundleMgr::Spawn() {
  SpawnFetcherPool();
  if (is_valid_)
    SpawnDispatcher();
}

bool BundleMgr::ScheduleTrigger(const PathString &path) {
  if (not is_valid_) {
    LogCvmfs(kLogBundleMgr,
             kLogDebug,
             "BundleMgr is not in a valid state. Can't schedule trigger!");
    return false;
  }
  // A single non-blocking attempt: prefetching is best-effort, so if the
  // trigger queue is full the request is dropped instead of stalling the
  // caller (an open() holding the remount fence).
  if (not TrySendPath(pipe_triggers_[1], path)) {
    LogCvmfs(kLogBundleMgr, kLogDebug, "trigger queue full, dropping %s",
             path.ToString().c_str());
    return false;
  }
  return true;
}

/**
 * Dependency paths in a bundle are absolute from the repository root.
 * Entries without a leading slash (optionally prefixed with "./") are
 * resolved relative to the directory holding the bundle file.
 */
PathString BundleMgr::NormalizeDependencyPath(const PathString &path,
                                              const PathString &parent_path) {
  if (path.StartsWith(PathString("/", 1)))
    return path;
  const PathString relative = path.StartsWith(PathString("./", 2))
                                  ? path.Suffix(2)
                                  : path;
  PathString normalized(parent_path);
  normalized.Append("/", 1);
  normalized.Append(relative.GetChars(), relative.GetLength());
  return normalized;
}

void BundleMgr::JoinFetcherPool() {
  if (pipe_bm_[1] < 0)
    return;
  // Send one kTerminate per worker. Workers drain all queued kFetch
  // messages before reaching their kTerminate (FIFO pipe), so we can't
  // just close the pipe — that would EOF some workers mid-drain.
  for (size_t i = 0; i < fetcher_threads_.size(); ++i) {
    Command cmd = Command::kTerminate;
    while (true) {
      const ssize_t n = ::write(pipe_bm_[1], &cmd, sizeof(Command));
      if (n == sizeof(Command))
        break;
      if (errno != EAGAIN && errno != EWOULDBLOCK)
        break;
    }
  }
  // Wait for every worker to drain its share of the queue and exit.
  for (auto &t : fetcher_threads_) {
    pthread_join(*t, nullptr);
  }
  fetcher_threads_.clear();
  ClosePipe(pipe_bm_);
  // Mark the pool as gone so that a subsequent call (e.g. the destructor
  // running after an explicit JoinFetcherPool()) is a no-op instead of a
  // double close/join.
  pipe_bm_[0] = pipe_bm_[1] = -1;
}

void BundleMgr::SpawnFetcherPool() {
  for (size_t i = 0; i < pool_size_; ++i) {
    std::unique_ptr<pthread_t> thread(new pthread_t());
    const int res = pthread_create(thread.get(), nullptr, MainBundleMgrFetcher,
                                   this);
    if (res != 0) {
      LogCvmfs(kLogBundleMgr, kLogDebug,
               "Thread creation failed! pool_size_=%zu spawned=%zu", pool_size_,
               i);
      is_valid_ = false;
      return;
    }
    fetcher_threads_.emplace_back(std::move(thread));
  }
}

void BundleMgr::SpawnDispatcher() {
  dispatcher_thread_.reset(new pthread_t());
  const int res = pthread_create(dispatcher_thread_.get(), nullptr,
                                 MainBundleMgrDispatcher, this);
  if (res != 0) {
    LogCvmfs(kLogBundleMgr, kLogDebug, "Dispatcher thread creation failed!");
    dispatcher_thread_.reset();
    is_valid_ = false;
  }
}

void BundleMgr::JoinDispatcher() {
  if (pipe_triggers_[1] < 0)
    return;
  // The dispatcher only exists once Spawn() has run; without it there is
  // just the pipe to close.
  if (dispatcher_thread_) {
    Command cmd = Command::kTerminate;
    while (true) {
      const ssize_t n = ::write(pipe_triggers_[1], &cmd, sizeof(Command));
      if (n == sizeof(Command))
        break;
      if (errno != EAGAIN && errno != EWOULDBLOCK)
        break;
    }
    pthread_join(*dispatcher_thread_, nullptr);
    dispatcher_thread_.reset();
  }
  ClosePipe(pipe_triggers_);
  pipe_triggers_[0] = pipe_triggers_[1] = -1;
}

/**
 * Loads the bundle spec that belongs to the given trigger file and enqueues
 * its dependencies for the fetcher pool. Runs on the dispatcher thread.
 */
void BundleMgr::ProcessTrigger(const PathString &trigger_path) {
  const NameString fname = GetFileName(trigger_path);
  const PathString parent_path = GetParentPath(trigger_path);
  // There is a naming convention regarding the name of the file with the
  // contents of the bundle
  const PathString bundle_file_path(parent_path.ToString() + "/.cvmfsbundle-"
                                    + fname.ToString());

  const std::unique_ptr<BundleFileMgr> bfm(
      LoadBundleFromCvmfs(mount_point_, bundle_file_path));
  if (bfm == nullptr) {
    LogCvmfs(kLogCvmfs, kLogDebug, "Couldn't fetch bundle associated to %s",
             trigger_path.ToString().c_str());
    return;
  }
  EnqueueDependencies(bfm.get(), parent_path);
}

void BundleMgr::EnqueueDependencies(BundleFileMgr *bfm,
                                    const PathString &parent_path) {
  while (auto file = bfm->GetNext()) {
    const PathString path = NormalizeDependencyPath(file, parent_path);
    // A single non-blocking attempt: if the dependency queue is full the
    // entry is dropped (prefetching is best-effort) instead of spinning.
    if (not TrySendPath(pipe_bm_[1], path)) {
      LogCvmfs(kLogBundleMgr, kLogDebug, "dependency queue full, dropping %s",
               path.ToString().c_str());
    }
  }
}

void BundleMgr::FetchPath(const PathString &path) {
  catalog::DirectoryEntry dirent;
  const bool found = mount_point_->catalog_mgr()->LookupPath(
      path, catalog::kLookupDefault, &dirent);
  cvmfs::Fetcher *this_fetcher = dirent.IsExternalFile()
                                     ? mount_point_->external_fetcher()
                                     : mount_point_->fetcher();
  if (not(found and this_fetcher)) {
    LogCvmfs(kLogCvmfs, kLogDebug,
             "BUNDLE-FETCH: lookup failed for %s (found=%d)",
             path.ToString().c_str(), int(found));
    return;
  }
  LogCvmfs(kLogCvmfs, kLogDebug, "BUNDLE-FETCH: prefetching %s",
           path.ToString().c_str());

  if (dirent.IsChunkedFile()) {
    // Files above the chunking threshold are stored as per-chunk objects;
    // their bulk object only exists if the repository sets
    // CVMFS_GENERATE_LEGACY_BULK_CHUNKS. Fetch the chunks, exactly like the
    // read path does.
    FileChunkList chunks;
    if (!mount_point_->catalog_mgr()->ListFileChunks(
            path, dirent.hash_algorithm(), &chunks)
        || chunks.IsEmpty()) {
      LogCvmfs(kLogCvmfs, kLogDebug, "BUNDLE-FETCH: no chunks found for %s",
               path.ToString().c_str());
      return;
    }
    for (unsigned i = 0; i < chunks.size(); ++i) {
      CacheManager::Label label;
      label.path = path.ToString();
      label.size = chunks.AtPtr(i)->size();
      label.zip_algorithm = dirent.compression_algorithm();
      label.flags |= CacheManager::kLabelChunked;
      if (mount_point_->catalog_mgr()->volatile_flag())
        label.flags |= CacheManager::kLabelVolatile;
      if (dirent.IsExternalFile()) {
        label.flags |= CacheManager::kLabelExternal;
        label.range_offset = chunks.AtPtr(i)->offset();
      }
      const int fd = this_fetcher->Fetch(
          CacheManager::LabeledObject(chunks.AtPtr(i)->content_hash(), label));
      if (fd >= 0)
        mount_point_->file_system()->cache_mgr()->Close(fd);
    }
    return;
  }

  CacheManager::Label label;
  label.path = path.ToString();
  label.size = dirent.size();
  label.zip_algorithm = dirent.compression_algorithm();
  if (mount_point_->catalog_mgr()->volatile_flag())
    label.flags |= CacheManager::kLabelVolatile;
  if (dirent.IsExternalFile())
    label.flags |= CacheManager::kLabelExternal;
  const int fd = this_fetcher->Fetch(
      CacheManager::LabeledObject(dirent.checksum(), label));
  if (fd >= 0)
    mount_point_->file_system()->cache_mgr()->Close(fd);
}

void *BundleMgr::MainBundleMgrDispatcher(void *data) {
#ifndef __APPLE__
  pthread_setname_np(pthread_self(), "bm_dispatch");
#endif
  BundleMgr *mgr = static_cast<BundleMgr *>(data);
  const int rfd = mgr->pipe_triggers_[0];

  // Single reader on this pipe, so no receive mutex is needed here
  while (true) {
    Command cmd = Command::kTerminate;
    const ssize_t n = read(rfd, &cmd, sizeof(Command));
    if (n != static_cast<ssize_t>(sizeof(Command)))
      break;
    if (cmd != Command::kFetch)
      break;
    const PathString path = mgr->ReceivePath(rfd);
    // While terminating, drain the queue without processing so that
    // unmounting does not wait for spec downloads
    if (atomic_read32(&mgr->terminating_) == 0)
      mgr->ProcessTrigger(path);
  }

  pthread_exit(nullptr);
}

void *BundleMgr::MainBundleMgrFetcher(void *data) {
#ifndef __APPLE__
  pthread_setname_np(pthread_self(), "bm_fetcher");
#endif
  BundleMgr *mgr = static_cast<BundleMgr *>(data);
  const int rfd = mgr->pipe_bm_[0];

  while (true) {
    Command cmd = Command::kTerminate;
    PathString path;
    bool got_path = false;
    bool eof = false;

    // Atomically receive cmd + (optional) path payload. The whole receipt
    // is under worker_read_mutex_ so messages aren't interleaved between
    // workers reading from the shared pipe.
    pthread_mutex_lock(&mgr->worker_read_mutex_);
    const ssize_t n = read(rfd, &cmd, sizeof(Command));
    if (n != static_cast<ssize_t>(sizeof(Command))) {
      eof = true;
    } else if (cmd == Command::kFetch) {
      path = mgr->ReceivePath(rfd);
      got_path = true;
    }
    pthread_mutex_unlock(&mgr->worker_read_mutex_);

    if (eof)
      break;

    bool terminate = false;
    switch (cmd) {
      case Command::kFetch: {
        if (!got_path) {
          terminate = true;
          break;
        }
        // While terminating, drain the queue without fetching so that
        // unmounting does not wait for pending downloads
        if (atomic_read32(&mgr->terminating_) == 0)
          mgr->FetchPath(path);
      } break;
      case Command::kTerminate:
      default:
        terminate = true;
        break;
    }
    if (terminate) {
      break;
    }
  }

  pthread_exit(nullptr);
}

PathString BundleMgr::ReceivePath(int fd) const {
  const std::string buffer = BlockingReceive(fd);
  assert(buffer.size() > 0 && "A path can't be empty");
  return PathString(buffer);
}

bool BundleMgr::TrySendPath(int fd, const PathString &path) const {
  // The whole message (command + length + payload) is sent as a single
  // write: per pipe(7), writes <= PIPE_BUF to a non-blocking pipe are
  // atomic, they either fully succeed or fail with EAGAIN. Sending the
  // parts separately could hit a full queue in the middle of a message
  // and corrupt the stream for all readers.
  const Command cmd = Command::kFetch;
  const size_t length = path.GetLength();
  const size_t msg_size = sizeof(cmd) + sizeof(length) + length;
  if (msg_size > PIPE_BUF) {
    LogCvmfs(kLogBundleMgr, kLogDebug,
             "path too long for the work queue, dropping %s",
             path.ToString().c_str());
    return false;
  }
  char msg[PIPE_BUF];
  memcpy(msg, &cmd, sizeof(cmd));
  memcpy(msg + sizeof(cmd), &length, sizeof(length));
  memcpy(msg + sizeof(cmd) + sizeof(length), path.GetChars(), length);

  const ssize_t n = write(fd, msg, msg_size);
  if (n == static_cast<ssize_t>(msg_size))
    return true;
  if ((n < 0) && not(errno == EAGAIN || errno == EWOULDBLOCK)) {
    LogCvmfs(kLogBundleMgr, kLogDebug,
             "write() on the work queue failed unexpectedly (errno=%d)",
             errno);
  }
  return false;
}
