/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_BUNDLE_MGR_H_
#define CVMFS_BUNDLE_MGR_H_
#include <gtest/gtest_prod.h>

#include <tuple>
#include <vector>

#include "file_bundle.h"
#include "mountpoint.h"
#include "shortstring.h"
#include "util/pointer.h"
#include "util/single_copy.h"

class BundleMgr : SingleCopy {
  friend class T_BundleMgr;
  FRIEND_TEST(T_BundleMgr, Simple);

 public:
  BundleMgr(MountPoint *mp, fuse_ino_t ino);
  virtual ~BundleMgr() { delete bfm_; }
  void Fetch();
  explicit operator bool() const { return is_valid_; }

 private:
  static void *EstablishConnection(void *data);
  void SpawnFetchers();
  void JoinFetchers();
  CacheManager::LabeledObject ReceiveLabeledObject(int fd) const;
  void SendLabeledObject(const CacheManager::LabeledObject &obj) const;

  MountPoint *mount_point_;
#ifndef __TEST_CVMFS_MOCKFUSE
  cvmfs::Fetcher *fetcher_;
#endif
  catalog::DirectoryEntry dirent_;
  PathString path_;
  NameString fname_;
  PathString parent_path_;

  // The file that contains the dependences
  PathString bundle_file_path_;
  BundleFileMgr *bfm_;

  std::vector<std::tuple<pthread_t, int> > fetcher_pool_;

  enum class Command {
    kTerminate,
    kFetch
  };

  /**
   * Used to send RPCs to the BundleMgr by the fetcher threads
   */
  int pipe_bm_[2];
  bool is_valid_ = true;
};
#endif  // CVMFS_BUNDLE_MGR_H_

