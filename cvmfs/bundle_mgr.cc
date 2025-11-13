/**
 * This file is part of the CernVM File System.
 */

#include "bundle_mgr.h"

#include <pthread.h>

#include <vector>

BundleMgr::BundleMgr(MountPoint *mp, fuse_ino_t ino) : is_valid_(true) {
    is_valid_ = cvmfs::GetPathForInode(mp, mp->file_system(), ino, &path_);
    is_valid_ &= cvmfs::GetDirentForInode(mp, mp->file_system(), ino, &dirent_);
    fname_ = GetFileName(path_);
    parent_path_ = GetParentPath(path_);

    // There is a naming convention regarding the name of the file with the
    // contents of the bundle
    bundle_file_path_ = PathString(parent_path_.ToString() + "/.cvmfsbundle."
                                   + fname_.ToString());

    bfm_ = new BundleFileMgr(bundle_file_path_);
  }

void BundleMgr::Fetch() {
  std::vector<pthread_t> fetcher_pool;

  for (PathString next_f = bfm_->GetNext(); not next_f.IsEmpty();
       next_f = bfm_->GetNext()) {
    pthread_t fetcher_t;

    // TODO(christge): DoFetch requires real arguments
    pthread_create(&fetcher_t, nullptr, DoFetch, nullptr);
    fetcher_pool.emplace_back(fetcher_t);
  }

  // Join fetcher threads
  while (not fetcher_pool.empty()) {
    for (auto it = fetcher_pool.begin(); it != fetcher_pool.end();) {
      auto &thread = *it;
      void *retval;
      int is_joinable = pthread_tryjoin_np(thread, &retval);
      if (is_joinable == 0) {
        // fetch has finished
        fetcher_pool.erase(it);
      } else {
        // still busy
        ++it;
      }
    }
  }
}

void* BundleMgr::DoFetch(void* data){}
