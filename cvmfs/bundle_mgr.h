/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_BUNDLE_MGR_H_
#define CVMFS_BUNDLE_MGR_H_

#include <tuple>
#include <type_traits>
#include <vector>

#include "duplex_testing.h"
#include "file_bundle.h"
#include "mountpoint.h"
#include "shortstring.h"
#include "util/pointer.h"
#include "util/single_copy.h"

class MockFetcher;

class BundleMgr : SingleCopy {
  friend class T_BundleMgr;
  FRIEND_TEST(T_BundleMgr, ExchangeLabeledObjects);
  FRIEND_TEST(T_BundleMgr, ExchangeCT);

 public:
  BundleMgr(MountPoint *mp, fuse_ino_t ino);
  virtual ~BundleMgr() { delete bfm_; }
  void Fetch();
  explicit operator bool() const { return is_valid_; }

 private:
  static void *MainBundleMgrFetcher(void *data);
  void SpawnFetchers();
  void JoinFetchers();
  UniquePtr<CacheManager::LabeledObject> ReceiveLabeledObject(int fd) const;
  bool SendLabeledObject(
      int fd, const UniquePtr<CacheManager::LabeledObject> &obj) const;
  bool TrySendData(int fd, UniquePtr<CacheManager::LabeledObject> &obj) const;

  // CT stands for contiguous type
  template<typename CT,
           typename = std::enable_if_t<std::is_trivially_copyable_v<CT> > >
  void BlockingSend(int fd, const CT &obj, size_t size = sizeof(CT)) const {
    using T = std::remove_cv_t<CT>;
    static_assert(
        std::is_trivially_copyable_v<T>,
        "Can't directly send non trivially copyable types over a pipe");
    static_assert(sizeof(T) == sizeof(CT), "CT illformed");
    static_assert(
        sizeof(T) <= PIPE_BUF,
        "Type too big to be guaranteed atomic transmission over a pipe");

    const T *ptr = reinterpret_cast<const T *>(&obj);
    while ((::write(fd, ptr, size)) != static_cast<ssize_t>(size)) {
      // Percist until succesfful write
    }
  }

  void BlockingSend(int fd, const std::string &string) const {
    const size_t size = string.size();
    BlockingSend(fd, size);
    while ((::write(fd, string.data(), size * sizeof(char)))
           != static_cast<ssize_t>(size * sizeof(char))) {
      // Percist until succesfful write
    }
  }

  template<typename CT,
           typename = std::enable_if_t<std::is_trivially_copyable_v<CT> > >
  CT BlockingReceive(int fd) const {
    using T = std::remove_cv_t<CT>;
    static_assert(
        sizeof(T) <= PIPE_BUF,
        "Type too big to be guaranteed atomic transmission over a pipe");
    CT item;
    ::read(fd, static_cast<void *>(&item), sizeof(CT));
    return item;
  }

  std::string BlockingReceive(int fd) const {
    const size_t size = BlockingReceive<size_t>(fd);
    assert(size * sizeof(char) < PIPE_BUF);
    std::string result(size, '\t');
    ::read(fd, static_cast<void *>(result.data()), size * sizeof(char));
    return result;
  }

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

