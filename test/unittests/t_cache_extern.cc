/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <alloca.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <string>

#include "cache.pb.h"
#include "cache_extern.h"
#include "cache_plugin/channel.h"
#include "cache_transport.h"
#include "hash.h"
#include "smalloc.h"
#include "util/posix.h"

using namespace std;  // NOLINT

/**
 * Receiving end of the cache manager.
 */
class MockCachePlugin : public CachePlugin {
 public:
  explicit MockCachePlugin(const string &socket_path) {
    bool retval = Listen(socket_path);
    assert(retval);
    ProcessRequests();
    known_object.algorithm = shash::kSha1;
    known_object_content = "Hello, World";
    shash::HashString(known_object_content, &known_object);
    known_object_refcnt = 0;
    next_status = -1;
  }

  virtual ~MockCachePlugin() { }

  string known_object_content;
  shash::Any known_object;
  shash::Any new_object;
  string new_object_content;
  int known_object_refcnt;
  int next_status;

 protected:
  virtual cvmfs::EnumStatus ChangeRefcount(
    const shash::Any &id,
    int32_t change_by)
  {
    if (next_status >= 0)
      return static_cast<cvmfs::EnumStatus>(next_status);
    if (id == new_object)
      return cvmfs::STATUS_OK;
    if (id == known_object) {
      if ((known_object_refcnt + change_by) < 0) {
        return cvmfs::STATUS_BADCOUNT;
      } else {
        known_object_refcnt += change_by;
        return cvmfs::STATUS_OK;
      }
    }
    return cvmfs::STATUS_NOENTRY;
  }

  virtual cvmfs::EnumStatus GetObjectInfo(
    const shash::Any &id,
    ObjectInfo *info)
  {
    if (next_status >= 0)
      return static_cast<cvmfs::EnumStatus>(next_status);
    if (id == known_object) {
      info->size = known_object_content.length();
      return cvmfs::STATUS_OK;
    }
    if (id == new_object) {
      info->size = new_object_content.length();
      return cvmfs::STATUS_OK;
    }
    return cvmfs::STATUS_NOENTRY;
  }

  virtual cvmfs::EnumStatus Pread(
    const shash::Any &id,
    uint64_t offset,
    uint32_t *size,
    unsigned char *buffer)
  {
    if (next_status >= 0)
      return static_cast<cvmfs::EnumStatus>(next_status);
    const char *data;
    unsigned data_size;
    if (id == known_object) {
      data = known_object_content.data();
      data_size = known_object_content.length();
    } else if (id == new_object) {
      data = new_object_content.data();
      data_size = new_object_content.length();
    } else {
      return cvmfs::STATUS_NOENTRY;
    }
    if (offset > data_size)
      return cvmfs::STATUS_OUTOFBOUNDS;
    *size = std::min(static_cast<uint64_t>(*size), data_size - offset);
    memcpy(buffer, data + offset, *size);
    return cvmfs::STATUS_OK;
  }

  virtual cvmfs::EnumStatus StartTxn(
    const shash::Any &id,
    const uint64_t txn_id,
    const ObjectInfo &info)
  {
    new_object = id;
    new_object_content.clear();
    return cvmfs::STATUS_OK;
  }

  virtual cvmfs::EnumStatus WriteTxn(
    const uint64_t txn_id,
    unsigned char *buffer,
    uint32_t size)
  {
    string data(reinterpret_cast<char *>(buffer), size);
    new_object_content += data;
    return cvmfs::STATUS_OK;
  }

  virtual cvmfs::EnumStatus CommitTxn(const uint64_t txn_id) {
    return cvmfs::STATUS_OK;
  }

  virtual cvmfs::EnumStatus AbortTxn(const uint64_t txn_id) {
    new_object_content.clear();
    return cvmfs::STATUS_OK;
  }
};


class T_ExternalCacheManager : public ::testing::Test {
 protected:
  virtual void SetUp() {
    socket_path_ = "cvmfs_cache_plugin.socket";
    mock_plugin_ = new MockCachePlugin(socket_path_);

    int fd_client = ConnectSocket(socket_path_);
    ASSERT_GE(fd_client, 0);
    cache_mgr_ = ExternalCacheManager::Create(fd_client, nfiles);
    ASSERT_TRUE(cache_mgr_ != NULL);
  }

  virtual void TearDown() {
    delete cache_mgr_;
    unlink(socket_path_.c_str());
    delete mock_plugin_;
  }

  static const int nfiles;
  string socket_path_;
  MockCachePlugin *mock_plugin_;
  ExternalCacheManager *cache_mgr_;
};

const int T_ExternalCacheManager::nfiles = 128;



TEST_F(T_ExternalCacheManager, Connection) {
  EXPECT_GE(cache_mgr_->session_id(), 0);
}


TEST_F(T_ExternalCacheManager, OpenClose) {
  EXPECT_EQ(-EBADF, cache_mgr_->Close(0));
  shash::Any rnd_id(shash::kSha1);
  rnd_id.Randomize();
  EXPECT_EQ(-ENOENT, cache_mgr_->Open(CacheManager::Bless(rnd_id)));

  int fds[nfiles];
  for (unsigned i = 0; i < nfiles; ++i) {
    fds[i] = cache_mgr_->Open(CacheManager::Bless(mock_plugin_->known_object));
    EXPECT_GE(fds[i], 0);
  }
  EXPECT_EQ(nfiles, mock_plugin_->known_object_refcnt);
  EXPECT_EQ(-ENFILE,
            cache_mgr_->Open(CacheManager::Bless(mock_plugin_->known_object)));
  for (unsigned i = 0; i < nfiles; ++i) {
    EXPECT_EQ(0, cache_mgr_->Close(fds[i]));
  }
  EXPECT_EQ(0, mock_plugin_->known_object_refcnt);

  mock_plugin_->next_status = cvmfs::STATUS_MALFORMED;
  EXPECT_EQ(-EINVAL,
            cache_mgr_->Open(CacheManager::Bless(mock_plugin_->known_object)));
  mock_plugin_->next_status = -1;
}


TEST_F(T_ExternalCacheManager, GetSize) {
  EXPECT_EQ(-EBADF, cache_mgr_->GetSize(0));
  int fd = cache_mgr_->Open(CacheManager::Bless(mock_plugin_->known_object));
  EXPECT_GE(fd, 0);
  EXPECT_EQ(static_cast<int64_t>(mock_plugin_->known_object_content.length()),
            cache_mgr_->GetSize(fd));

  mock_plugin_->next_status = cvmfs::STATUS_MALFORMED;
  EXPECT_EQ(-EINVAL, cache_mgr_->GetSize(fd));
  mock_plugin_->next_status = -1;

  EXPECT_EQ(0, cache_mgr_->Close(fd));
}


TEST_F(T_ExternalCacheManager, Dup) {
  EXPECT_EQ(-EBADF, cache_mgr_->Dup(0));
  int fds[nfiles];
  fds[0] = cache_mgr_->Open(CacheManager::Bless(mock_plugin_->known_object));
  EXPECT_GE(fds[0], 0);
  for (unsigned i = 1; i < nfiles; ++i) {
    fds[i] = cache_mgr_->Dup(fds[0]);
    EXPECT_GE(fds[i], 0);
  }
  EXPECT_EQ(nfiles, mock_plugin_->known_object_refcnt);
  EXPECT_EQ(-ENFILE, cache_mgr_->Dup(fds[0]));
  for (unsigned i = 0; i < nfiles; ++i) {
    EXPECT_EQ(0, cache_mgr_->Close(fds[i]));
  }
  EXPECT_EQ(0, mock_plugin_->known_object_refcnt);
}


TEST_F(T_ExternalCacheManager, Pread) {
  unsigned buf_size = 64;
  char buffer[64];
  EXPECT_EQ(-EBADF, cache_mgr_->Pread(0, buffer, buf_size, 0));

  int fd = cache_mgr_->Open(CacheManager::Bless(mock_plugin_->known_object));
  EXPECT_GE(fd, 0);
  EXPECT_EQ(-EINVAL, cache_mgr_->Pread(fd, buffer, 1, 64));
  int64_t len = cache_mgr_->Pread(fd, buffer, 64, 0);
  EXPECT_EQ(static_cast<int>(mock_plugin_->known_object_content.length()), len);
  EXPECT_EQ(mock_plugin_->known_object_content, string(buffer, len));
  EXPECT_EQ(1, cache_mgr_->Pread(fd, buffer, 1, len-1));
  EXPECT_EQ(mock_plugin_->known_object_content[len-1], buffer[0]);
  EXPECT_EQ(0, cache_mgr_->Close(fd));
}


TEST_F(T_ExternalCacheManager, Readahead) {
  EXPECT_EQ(-EBADF, cache_mgr_->Readahead(0));
  int fd = cache_mgr_->Open(CacheManager::Bless(mock_plugin_->known_object));
  EXPECT_GE(fd, 0);
  EXPECT_EQ(0, cache_mgr_->Readahead(fd));
  EXPECT_EQ(0, cache_mgr_->Close(fd));
}


TEST_F(T_ExternalCacheManager, Transaction) {
  shash::Any id(shash::kSha1);
  string content = "foo";
  HashString(content, &id);
  unsigned char *data = const_cast<unsigned char *>(
    reinterpret_cast<const unsigned char *>(content.data()));
  EXPECT_TRUE(
    cache_mgr_->CommitFromMem(id, data, content.length(), "test"));
  unsigned char *buffer;
  uint64_t size;
  EXPECT_TRUE(cache_mgr_->Open2Mem(id, "test", &buffer, &size));
  EXPECT_EQ(content, string(reinterpret_cast<char *>(buffer), size));
  free(buffer);

  content = "";
  HashString(content, &id);
  data = NULL;
  EXPECT_TRUE(
    cache_mgr_->CommitFromMem(id, data, content.length(), "test"));
  EXPECT_TRUE(cache_mgr_->Open2Mem(id, "test", &buffer, &size));
  EXPECT_EQ(0U, size);
  EXPECT_EQ(NULL, buffer);

  unsigned large_size = 50 * 1024 * 1024;
  unsigned char *large_buffer = reinterpret_cast<unsigned char *>(
    scalloc(large_size, 1));
  EXPECT_TRUE(
    cache_mgr_->CommitFromMem(id, large_buffer, large_size, "test"));
  unsigned char *large_buffer_verify = reinterpret_cast<unsigned char *>(
    smalloc(large_size));
  EXPECT_TRUE(cache_mgr_->Open2Mem(id, "test", &large_buffer_verify, &size));
  EXPECT_EQ(large_size, size);
  EXPECT_EQ(0, memcmp(large_buffer, large_buffer_verify, large_size));
  free(large_buffer_verify);
  free(large_buffer);

  large_size = 50 * 1024 * 1024 + 1;
  large_buffer = reinterpret_cast<unsigned char *>(scalloc(large_size, 1));
  EXPECT_TRUE(
    cache_mgr_->CommitFromMem(id, large_buffer, large_size, "test"));
  large_buffer_verify = reinterpret_cast<unsigned char *>(smalloc(large_size));
  EXPECT_TRUE(cache_mgr_->Open2Mem(id, "test", &large_buffer_verify, &size));
  EXPECT_EQ(large_size, size);
  EXPECT_EQ(0, memcmp(large_buffer, large_buffer_verify, large_size));
  free(large_buffer_verify);
  free(large_buffer);

  // test unordered upload of chunks (and failure inbetween)
}


TEST_F(T_ExternalCacheManager, TransactionAbort) {
  shash::Any id(shash::kSha1);
  string content = "foo";
  HashString(content, &id);
  void *txn = alloca(cache_mgr_->SizeOfTxn());
  EXPECT_EQ(0, cache_mgr_->StartTxn(id, content.length(), txn));
  EXPECT_EQ(0, cache_mgr_->Reset(txn));
  EXPECT_EQ(2, cache_mgr_->Write(content.data(), 2, txn));
  EXPECT_EQ(0, cache_mgr_->Reset(txn));
  EXPECT_EQ(3, cache_mgr_->Write(content.data(), 3, txn));
  EXPECT_EQ(0, cache_mgr_->CommitTxn(txn));

  unsigned char *buf;
  uint64_t size;
  EXPECT_TRUE(cache_mgr_->Open2Mem(id, "test", &buf, &size));
  EXPECT_EQ(content, string(reinterpret_cast<char *>(buf), size));
  free(buf);
}

namespace {

struct ThreadData {
  ExternalCacheManager *cache_mgr;
  unsigned large_size;
  unsigned char *large_buffer;
  shash::Any id;
};

static void *MainMultiThread(void *data) {
  ThreadData *td = reinterpret_cast<ThreadData *>(data);

  uint64_t size;
  unsigned char *buffer;
  EXPECT_TRUE(td->cache_mgr->Open2Mem(td->id, "test", &buffer, &size));
  EXPECT_EQ(td->large_size, size);
  EXPECT_EQ(0, memcmp(buffer, td->large_buffer, size));
  free(buffer);
  return NULL;
}

}  // anonymous namespace

TEST_F(T_ExternalCacheManager, MultiThreaded) {
  cache_mgr_->Spawn();

  unsigned large_size = 50 * 1024 * 1024;
  unsigned char *large_buffer = reinterpret_cast<unsigned char *>(
    smalloc(large_size));
  memset(large_buffer, 1, large_size);
  shash::Any id(shash::kSha1);
  shash::HashMem(large_buffer, large_size, &id);
  EXPECT_TRUE(
    cache_mgr_->CommitFromMem(id, large_buffer, large_size, "test"));

  unsigned num_threads = 10;
  pthread_t threads[num_threads];
  ThreadData td[num_threads];
  for (unsigned i = 0; i < num_threads; ++i) {
    td[i].cache_mgr = cache_mgr_;
    td[i].large_size = large_size;
    td[i].large_buffer = large_buffer;
    td[i].id = id;

    int retval = pthread_create(&threads[i], NULL, MainMultiThread, &td[i]);
    assert(retval == 0);
  }
  for (unsigned i = 0; i < num_threads; ++i) {
    pthread_join(threads[i], NULL);
  }
}
