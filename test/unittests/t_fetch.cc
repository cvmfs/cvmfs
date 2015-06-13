/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <fcntl.h>
#include <pthread.h>

#include "../../cvmfs/cache.h"
#include "../../cvmfs/download.h"
#include "../../cvmfs/fetch.h"
#include "../../cvmfs/hash.h"
#include "../../cvmfs/util.h"
#include "testutil.h"

using namespace std;  // NOLINT

namespace cvmfs {

class T_Fetcher : public ::testing::Test {
 protected:
  virtual void SetUp() {
    used_fds_ = GetNoUsedFds();
    for (unsigned i = 0; i < 8; ++i) {
      hashes_.push_back(shash::Any(shash::kSha1));
      hashes_[i].digest[0] = i;
    }

    tmp_path_ = CreateTempDir("/tmp/cvmfs_test");
    cache_mgr_ = cache::PosixCacheManager::Create(tmp_path_, false);
    ASSERT_TRUE(cache_mgr_ != NULL);

    download_mgr_ = new download::DownloadManager();
    download_mgr_->Init(8, false, /* use_system_proxy */ &statistics);

    fetcher_ = new Fetcher(cache_mgr_, download_mgr_);
  }

  virtual void TearDown() {
    delete fetcher_;
    download_mgr_->Fini();
    delete download_mgr_;
    delete cache_mgr_;
    if (tmp_path_ != "")
      RemoveTree(tmp_path_);
    EXPECT_EQ(used_fds_, GetNoUsedFds());
  }

  Fetcher *fetcher_;
  cache::PosixCacheManager *cache_mgr_;
  perf::Statistics statistics;
  download::DownloadManager *download_mgr_;
  unsigned used_fds_;
  vector<shash::Any> hashes_;
  string tmp_path_;
};


void *TestGetTls(void *data) {
  Fetcher *f = static_cast<Fetcher *>(data);
  void *thread_tls = f->GetTls();
  EXPECT_TRUE(thread_tls != NULL);
  EXPECT_EQ(thread_tls, f->GetTls());
  EXPECT_EQ(2U, f->tls_blocks_.size());
  return thread_tls;
}

TEST_F(T_Fetcher, GetTls) {
  void *this_tls = fetcher_->GetTls();
  EXPECT_TRUE(this_tls != NULL);
  // Idempotent
  EXPECT_EQ(this_tls, fetcher_->GetTls());
  EXPECT_EQ(1U, fetcher_->tls_blocks_.size());

  pthread_t thread;
  EXPECT_EQ(0, pthread_create(&thread, NULL, TestGetTls, fetcher_));
  void *other_thread_tls;
  pthread_join(thread, &other_thread_tls);
  //EXPECT_TRUE(other_thread_tls != NULL);
  //EXPECT_NE(other_thread_tls, this_tls);
}


TEST_F(T_Fetcher, SignalWaitingThreads) {
  unsigned char x = 'x';
  EXPECT_TRUE(cache_mgr_->CommitFromMem(hashes_[0], &x, 1, ""));
  int fd = cache_mgr_->Open(hashes_[0]);
  EXPECT_GE(fd, 0);
  int tls_pipe[2];
  MakePipe(tls_pipe);

  fetcher_->queues_download_[hashes_[0]] = NULL;
  fetcher_->queues_download_[hashes_[1]] = NULL;
  fetcher_->queues_download_[hashes_[2]] = NULL;

  fetcher_->GetTls()->other_pipes_waiting.push_back(tls_pipe[1]);
  fetcher_->SignalWaitingThreads(-1, hashes_[0], fetcher_->GetTls());
  EXPECT_EQ(0U, fetcher_->queues_download_.count(hashes_[0]));

  fetcher_->GetTls()->other_pipes_waiting.push_back(tls_pipe[1]);
  fetcher_->SignalWaitingThreads(fd, hashes_[1], fetcher_->GetTls());
  EXPECT_EQ(0U, fetcher_->queues_download_.count(hashes_[1]));

  fetcher_->GetTls()->other_pipes_waiting.push_back(tls_pipe[1]);
  fetcher_->SignalWaitingThreads(1000000, hashes_[2], fetcher_->GetTls());
  EXPECT_EQ(0U, fetcher_->queues_download_.count(hashes_[2]));

  int fd_return0;
  int fd_return1;
  int fd_return2;
  ReadPipe(tls_pipe[0], &fd_return0, sizeof(fd_return0));
  ReadPipe(tls_pipe[0], &fd_return1, sizeof(fd_return1));
  ReadPipe(tls_pipe[0], &fd_return2, sizeof(fd_return2));
  EXPECT_EQ(-1, fd_return0);
  EXPECT_NE(fd, fd_return1);
  EXPECT_EQ(0, cache_mgr_->Close(fd_return1));
  EXPECT_EQ(-EBADF, fd_return2);

  ClosePipe(tls_pipe);
  EXPECT_EQ(0, cache_mgr_->Close(fd));
}

}  // namespace cvmfs
