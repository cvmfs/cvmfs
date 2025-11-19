/**
 * This file is part of the CernVM File System.
 */

#include "bundle_mgr.h"

#include <pthread.h>

#include <vector>

#include "fetch.h"
#include "util/inode.h"
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

  CacheManager::LabeledObject *obj;
  while ((obj = bfm_->GetNext()) != nullptr) {
    // TODO(christge): In this form the assignment of a fetcher is left to
    // SendLabeledObject. This is incorrect. SendLabeledObject should only
    // unpack the LabeledObject and send it over the pipe
    SendLabeledObject(*obj);
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
      LogCvmfs(kLogBungleMgr,
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
      int res = pthread_timedjoin_np(thread, nullptr, &ts);
      if (res != 0) {
        LogCvmfs(kLogBungleMgr,
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

  size_t size = bfm_->Size() / 30;
  for (size_t i = 0; i < size; ++i) {
    pthread_t thread;
    int res = pthread_create(&thread, nullptr, EstablishConnection, this);
    if (res != 0) {
      continue;
    }
    int fd;
    ReadPipe(pipe_bm_[0], &fd, sizeof(int));
    fetcher_pool_.push_back({thread, fd});
  }
}

void *BundleMgr::EstablishConnection(void *data) {
  pthread_setname_np(pthread_self(), "bm_fetcher");
  BundleMgr *mgr = static_cast<BundleMgr *>(data);
  int rfd = mgr->pipe_bm_[1];
  int back_channel[2];
  MakePipe(back_channel);

  WritePipe(rfd, &back_channel[1], sizeof(int));

  Command cmd;
  while (read(rfd, &cmd, sizeof(Command)) == sizeof(Command)) {
    bool terminate = false;
    switch (cmd) {
      case Command::kFetch: {
        auto object = mgr->ReceiveLabeledObject(rfd);
        mgr->fetcher_->Fetch(object);
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

CacheManager::LabeledObject BundleMgr::ReceiveLabeledObject(int fd) const {
  return CacheManager::LabeledObject(shash::Any{}, CacheManager::Label());
}

void BundleMgr::SendLabeledObject(
    const CacheManager::LabeledObject &obj) const {
  (void)obj;
}

