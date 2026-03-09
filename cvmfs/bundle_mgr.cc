/**
 * This file is part of the CernVM File System.
 */

#include "bundle_mgr.h"

#include <pthread.h>

#include <cassert>

#include "catalog_mgr_client.h"
#include "fetch.h"
#include "mountpoint.h"
#include "options.h"
#include "shortstring.h"
#include "util/posix.h"

BundleMgr::BundleMgr(MountPoint *mp, const PathString &path)
    : mount_point_(mp), path_(path) {
  fname_ = GetFileName(path_);
  parent_path_ = GetParentPath(path_);
  // There is a naming convention regarding the name of the file with the
  // contents of the bundle
  bundle_file_path_ = PathString(parent_path_.ToString() + "/.cvmfsbundle."
                                 + fname_.ToString());

  bfm_ = new BundleFileMgr(bundle_file_path_);
  pipe_bm_[0] = pipe_bm_[1] = -1;
  SpawnFetcher();
}

void BundleMgr::Fetch() {
  if (not is_valid_) {
    LogCvmfs(kLogBundleMgr,
             kLogDebug,
             "BundleMgr is not in a valid state. Can't fetch!");
    return;
  }

  while (auto file = bfm_->GetNext()) {
    // TODO(christge): Make sure this TrySend is actually needed
    while (not TrySendPath(back_channel_, file)) {
    }
  }
}

void BundleMgr::JoinFetcher() {
  Command cmd = Command::kTerminate;
  WritePipe(back_channel_, &cmd, sizeof(Command));
  pthread_detach(*fetcher_thread_);
  ClosePipe(pipe_bm_);
  delete fetcher_thread_;
}

void BundleMgr::SpawnFetcher() {
  MakePipe(pipe_bm_);

  fetcher_thread_ = new pthread_t;
  const int res = pthread_create(
      fetcher_thread_, nullptr, MainBundleMgrFetcher, this);
  if (res != 0) {
    LogCvmfs(kLogBundleMgr, kLogDebug, "Thread creation failed!");
    is_valid_ = false;
    return;
  }
  ReadPipe(pipe_bm_[0], &back_channel_, sizeof(int));

  // Make the write operation to the return pipe non blocking
  // According to the man (7) page of write, when attempting to write
  // n<=PIPE_BUF data on a non blocking pipe, it will either write all of them
  // or errno will be set to EAGAIN. PIPE_BUF is at least 512bytes on and
  // linux 4096bytes.
  const int flags = fcntl(back_channel_, F_GETFL);
  fcntl(back_channel_, F_SETFL, flags | O_NONBLOCK);
}

void BundleMgr::FetchPath(const PathString &path) {
  catalog::DirectoryEntry dirent;
  const bool found = mount_point_->catalog_mgr()->LookupPath(
      path, catalog::kLookupDefault, &dirent);
  cvmfs::Fetcher *this_fetcher = dirent.IsExternalFile()
                                     ? mount_point_->external_fetcher()
                                     : mount_point_->fetcher();
  if (not(found and this_fetcher)) {
    // The path should be resolved to a valid dirent and a fetcher should be
    // available
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
  this_fetcher->Fetch(CacheManager::LabeledObject(dirent.checksum(), label));
}

void *BundleMgr::MainBundleMgrFetcher(void *data) {
  pthread_setname_np(pthread_self(), "bm_fetcher");
  BundleMgr *mgr = static_cast<BundleMgr *>(data);
  const int wfd = mgr->pipe_bm_[1];
  int back_channel[2];
  MakePipe(back_channel);

  WritePipe(wfd, &back_channel[1], sizeof(int));

  const int rfd = back_channel[0];

  Command cmd;
  while (read(rfd, &cmd, sizeof(Command)) == sizeof(Command)) {
    bool terminate = false;
    switch (cmd) {
      case Command::kFetch:
        {
          auto path = mgr->ReceivePath(rfd);
          std::string cvmfs_mount_dir;
          if (not mgr->mount_point_->file_system()->options_mgr()->GetValue(
                  "CVMFS_MOUNT_DIR", &cvmfs_mount_dir)) {
            LogCvmfs(kLogBundleMgr,
                     kLogDebug | kLogSyslogErr,
                     "CVMFS_MOUNT_DIR missing");
            terminate = true;
          } else {
            auto full_path = cvmfs_mount_dir + "/" + path.ToString();
            mgr->FetchPath(PathString(full_path));
          }
        }
        break;
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

