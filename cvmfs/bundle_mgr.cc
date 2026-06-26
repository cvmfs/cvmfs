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
#include <memory>
#include <string>
#include <vector>

#include "cache.h"
#include "catalog_mgr_client.h"
#include "fetch.h"
#include "json_document.h"
#include "mountpoint.h"
#include "options.h"
#include "shortstring.h"
#include "util/posix.h"

namespace {
constexpr size_t kDefaultBundlePoolSize = 8;

// Read the .cvmfsbundle.<basename> file via the cvmfs cache
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

BundleMgr::BundleMgr(MountPoint *mp, const PathString &path)
    : mount_point_(mp)
    , path_(path)
    , fetcher_threads_()
    , pool_size_(kDefaultBundlePoolSize) {
  fname_ = GetFileName(path_);
  parent_path_ = GetParentPath(path_);
  // There is a naming convention regarding the name of the file with the
  // contents of the bundle
  bundle_file_path_ = PathString(parent_path_.ToString() + "/.cvmfsbundle."
                                 + fname_.ToString());

  pipe_bm_[0] = pipe_bm_[1] = -1;
  pthread_mutex_init(&worker_read_mutex_, nullptr);

  bfm_ = LoadBundleFromCvmfs(mount_point_, bundle_file_path_);
  if (bfm_ == nullptr) {
    LogCvmfs(kLogCvmfs, kLogDebug, "BundleMgr: failed to load bundle file %s",
             bundle_file_path_.ToString().c_str());
    is_valid_ = false;
    return;
  }

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

  SpawnFetcherPool();
}

void BundleMgr::Fetch() {
  if (not is_valid_) {
    LogCvmfs(kLogBundleMgr,
             kLogDebug,
             "BundleMgr is not in a valid state. Can't fetch!");
    return;
  }

  while (auto file = bfm_->GetNext()) {
    // A TrySendPath() here is used as a profylaxis to a scenario where the pipe
    // is currently blocked.
    while (not TrySendPath(back_channel_, file)) {
    }
  }
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
  MakePipe(pipe_bm_);
  back_channel_ = pipe_bm_[1];

  // Non-blocking writes on the work-queue pipe so TrySendPath can poll.
  // Per pipe(7), writes <= PIPE_BUF are atomic on non-blocking pipes:
  // they either fully succeed or fail with EAGAIN.
  const int flags = fcntl(back_channel_, F_GETFL);
  fcntl(back_channel_, F_SETFL, flags | O_NONBLOCK);

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

  CacheManager::Label label;
  label.path = path.ToString();
  label.size = dirent.size();
  label.zip_algorithm = dirent.compression_algorithm();
  if (mount_point_->catalog_mgr()->volatile_flag())
    label.flags |= CacheManager::kLabelVolatile;
  if (dirent.IsExternalFile())
    label.flags |= CacheManager::kLabelExternal;
  this_fetcher->Fetch(CacheManager::LabeledObject(dirent.checksum(), label));
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
  Command cmd = Command::kFetch;
  if ((write(fd, &cmd, sizeof(Command))) != sizeof(Command)) {
    if (not(errno == EAGAIN || errno == EWOULDBLOCK)) {
      LogCvmfs(kLogBundleMgr,
               kLogDebug,
               "write() on back channel failed unexpectedly");
    }
    return false;
  } else {
    BlockingSend(fd, path);
  }
  return true;
}

