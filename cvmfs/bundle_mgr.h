/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_BUNDLE_MGR_H_
#define CVMFS_BUNDLE_MGR_H_

#include <limits.h>
#include <pthread.h>

#include <cassert>
#include <cstddef>
#include <memory>
#include <type_traits>
#include <vector>

#include "duplex_testing.h"
#include "file_bundle.h"
#include "mountpoint.h"
#include "shortstring.h"
#include "util/posix.h"
#include "util/single_copy.h"

class MockFetcher;

class BundleMgr : SingleCopy {
  friend class T_BundleMgr;
  FRIEND_TEST(T_BundleMgr, ExchangeCT);
  FRIEND_TEST(T_BundleMgr, ExchangePathString);
  FRIEND_TEST(T_BundleMgr, Fetch);

 public:
  BundleMgr(MountPoint *mp, const PathString &path);
  virtual ~BundleMgr() {
    JoinFetcherPool();
    pthread_mutex_destroy(&worker_read_mutex_);
    delete bfm_;
  }
  void Fetch();
  explicit operator bool() const { return is_valid_; }

 private:
  static void *MainBundleMgrFetcher(void *data);
  void SpawnFetcherPool();
  void JoinFetcherPool();
  PathString ReceivePath(int fd) const;
  bool TrySendPath(int fd, const PathString &path) const;

  void FetchPath(const PathString &path);

  // CT stands for contiguous type
  template<typename CT,
           typename = typename std::enable_if<
               std::is_trivially_copyable<CT>::value>::type>
  void BlockingSend(int fd, const CT &obj, size_t size = sizeof(CT)) const {
    typedef typename std::remove_cv<CT>::type T;
    static_assert(
        std::is_trivially_copyable<T>::value,
        "Can't directly send non trivially copyable types over a pipe");
    static_assert(sizeof(T) == sizeof(CT), "CT illformed");
    static_assert(
        sizeof(T) <= PIPE_BUF,
        "Type too big to be guaranteed atomic transmission over a pipe");

    const T *ptr = reinterpret_cast<const T *>(&obj);
    WritePipe(fd, ptr, size);
  }

  void BlockingSend(int fd, const PathString &path) const {
    const size_t size = path.GetLength();
    BlockingSend(fd, size);
    WritePipe(fd, path.GetChars(), size * sizeof(char));
  }

  void BlockingSend(int fd, const std::string &string) const {
    const size_t size = string.size();
    BlockingSend(fd, size);
    WritePipe(fd, string.data(), size * sizeof(char));
  }

  template<typename CT,
           typename = typename std::enable_if<
               std::is_trivially_copyable<CT>::value>::type>
  CT BlockingReceive(int fd) const {
    typedef typename std::remove_cv<CT>::type T;
    static_assert(
        sizeof(T) <= PIPE_BUF,
        "Type too big to be guaranteed atomic transmission over a pipe");
    CT item;
    ReadPipe(fd, static_cast<void *>(&item), sizeof(CT));
    return item;
  }

  std::string BlockingReceive(int fd) const {
    const size_t size = BlockingReceive<size_t>(fd);
    assert(size * sizeof(char) < PIPE_BUF);
    std::string result(size, '\t');
    ReadPipe(fd, static_cast<void *>(&result[0]), size * sizeof(char));
    return result;
  }

  MountPoint *mount_point_;
  PathString path_;
  NameString fname_;
  PathString parent_path_;

  // The file that contains the dependences
  PathString bundle_file_path_;
  BundleFileMgr *bfm_;

  // Pool of fetcher threads. All workers share pipe_bm_[0] (read end)
  // and serialize their reads via worker_read_mutex_ so cmd+payload
  // pairs are received atomically.
  std::vector<std::unique_ptr<pthread_t> > fetcher_threads_;
  pthread_mutex_t worker_read_mutex_;
  size_t pool_size_;
  int back_channel_;

  enum class Command {
    kTerminate,
    kFetch
  };

  /**
   * Work queue (a pipe). Main thread writes Command + path payload to
   * pipe_bm_[1]; workers read from pipe_bm_[0] under worker_read_mutex_.
   */
  int pipe_bm_[2];
  bool is_valid_ = true;
};
#endif  // CVMFS_BUNDLE_MGR_H_

