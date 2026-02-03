/**
 * This file is part of the CernVM File System.
 */

#include "bundle_mgr.h"

#include <pthread.h>

#include <vector>

#include "fetch.h"
#include "util/inode.h"
#include "util/pointer.h"
#include "util/posix.h"

BundleMgr::BundleMgr(MountPoint *mp, fuse_ino_t ino) : mount_point_(mp) {
  is_valid_ = cvmfs::GetPathForInode(mp, mp->file_system(), ino, &path_);
  is_valid_ &= cvmfs::GetDirentForInode(mp, mp->file_system(), ino, &dirent_);
  if (not is_valid_) {
    return;
  }
  fname_ = GetFileName(path_);
  parent_path_ = GetParentPath(path_);
  fetcher_ = dirent_.IsExternalFile() ? mp->external_fetcher() : mp->fetcher();
  // There is a naming convention regarding the name of the file with the
  // contents of the bundle
  bundle_file_path_ = PathString(parent_path_.ToString() + "/.cvmfsbundle."
                                 + fname_.ToString());

  bfm_ = new BundleFileMgr(bundle_file_path_);
  pipe_bm_[0] = pipe_bm_[1] = -1;
}

void BundleMgr::Fetch() {
  SpawnFetchers();

  auto it = fetcher_pool_.begin();
  if (it == fetcher_pool_.end()) {
    LogCvmfs(kLogBundleMgr,
             kLogDebug,
             "The pool of fetchers is empty. Can't fetch dependencies. "
             "Aborting Op.");
    return;
  }

  while (true) {
    UniquePtr<CacheManager::LabeledObject> obj = bfm_->GetNext();
    if (not obj.IsValid()) {
      break;
    }
    auto wfd = std::get<1>(*it);
    // Find the first available Fetcher to send the data
    while (not TrySendData(wfd, obj)) {
      if ((++it) == fetcher_pool_.end()) {
        it = fetcher_pool_.begin();
      }
      wfd = std::get<1>(*it);
    }
    if ((++it) == fetcher_pool_.end()) {
      it = fetcher_pool_.begin();
    }
  }

  JoinFetchers();
}

void BundleMgr::JoinFetchers() {
  bool detach_mode = false;
  // Join fetcher threads.
  // Give a fetcher thread 10 seconds to join.
  for (auto it = fetcher_pool_.begin(); it != fetcher_pool_.end(); ++it) {
    auto &tuple = *it;
    auto thread = std::get<0>(tuple);
    auto fd = std::get<1>(tuple);

    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) == -1) {
      LogCvmfs(kLogBundleMgr,
               kLogDebug,
               "Failed to read CLOCK_REALTIME. Detaching fetchers.");
      detach_mode = true;
    }

    if (detach_mode) {
      pthread_detach(thread);
    } else {
      ts.tv_sec += 10;
      Command cmd = Command::kTerminate;
      WritePipe(fd, &cmd, sizeof(Command));
      if (pthread_timedjoin_np(thread, nullptr, &ts) != 0) {
        LogCvmfs(kLogBundleMgr,
                 kLogDebug,
                 "Fetcher is busy for too long. Detaching.");
        pthread_detach(thread);
      }
    }
  }
  ClosePipe(pipe_bm_);
}

void BundleMgr::SpawnFetchers() {
  MakePipe(pipe_bm_);

  const size_t size = 1 + (bfm_->Size() / 30);  // Spawn at least one fetcher
  for (size_t i = 0; i < size; ++i) {
    pthread_t thread;
    const int res = pthread_create(&thread, nullptr, EstablishConnection, this);
    if (res != 0) {
      LogCvmfs(kLogBundleMgr, kLogDebug, "Thread creation failed!");
      continue;
    }
    int fd;
    ReadPipe(pipe_bm_[0], &fd, sizeof(int));

    // Make the write operation to the return pipe non blocking
    // According to the man (7) page of write, when attempting to write
    // n<=PIPE_BUF data on a non blocking pipe, it will either write all of them
    // or errno will be set to EAGAIN. PIPE_BUF is at least 512bytes on and
    // linux 4096bytes.
    const int flags = fcntl(fd, F_GETFL);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    fetcher_pool_.push_back({thread, fd});
  }
}

void *BundleMgr::EstablishConnection(void *data) {
  pthread_setname_np(pthread_self(), "bm_fetcher");
  BundleMgr *mgr = static_cast<BundleMgr *>(data);
  const int& wfd = mgr->pipe_bm_[1];
  int back_channel[2];
  MakePipe(back_channel);

  WritePipe(wfd, &back_channel[1], sizeof(int));

  const int &rfd = back_channel[0];

  Command cmd;
  while (read(rfd, &cmd, sizeof(Command)) == sizeof(Command)) {
    bool terminate = false;
    switch (cmd) {
      case Command::kFetch: {
        auto obj = mgr->ReceiveLabeledObject(rfd);
        if (not obj.IsValid()) {
          LogCvmfs(kLogBundleMgr, kLogDebug, "Received a null object");
          break;
        }
        mgr->fetcher_->Fetch(*obj);
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

  ClosePipe(back_channel);
  pthread_exit(nullptr);
}

UniquePtr<CacheManager::LabeledObject> BundleMgr::ReceiveLabeledObject(
    int fd) const {
  const shash::Any id = BlockingReceive<shash::Any>(fd);
  CacheManager::Label label;
  label.flags = BlockingReceive<int>(fd);
  label.size = BlockingReceive<uint64_t>(fd);
  label.zip_algorithm = BlockingReceive<zlib::Algorithms>(fd);
  label.range_offset = BlockingReceive<off_t>(fd);
  label.path = BlockingReceive(fd);
  CacheManager::LabeledObject *obj = new CacheManager::LabeledObject(id, label);
  assert(obj != nullptr);
  return UniquePtr<CacheManager::LabeledObject>(obj);
}

bool BundleMgr::SendLabeledObject(
    int fd, const UniquePtr<CacheManager::LabeledObject> &obj) const {
  BlockingSend(fd, obj->id);
  BlockingSend(fd, obj->label.flags);
  BlockingSend(fd, obj->label.size);
  BlockingSend(fd, obj->label.zip_algorithm);
  BlockingSend(fd, obj->label.range_offset);
  const std::string &path = obj->label.path;
  BlockingSend(fd, path);
  return true;
}

bool BundleMgr::TrySendData(int fd,
                            UniquePtr<CacheManager::LabeledObject> &obj) const {
  Command cmd = Command::kFetch;
  if ((write(fd, &cmd, sizeof(Command))) != sizeof(Command)) {
    if (not(errno == EAGAIN || errno == EWOULDBLOCK)) {
      LogCvmfs(kLogBundleMgr,
               kLogDebug,
               "write() on back channel failed unexpectedly");
    }
    return false;
  } else {
    while (SendLabeledObject(fd, obj) != true) {
      // If a Fetcher receives a kFetch command should receive the Labeled
      // Object also.
    }
  }

  return true;
}

