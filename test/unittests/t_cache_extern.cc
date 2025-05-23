/**
 * This file is part of the CernVM File System.
 */

#include <alloca.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <map>
#include <string>
#include <vector>

#include "cache.pb.h"
#include "cache_extern.h"
#include "cache_plugin/channel.h"
#include "cache_transport.h"
#include "crypto/hash.h"
#include "util/posix.h"
#include "util/smalloc.h"

using namespace std;  // NOLINT

/**
 * Receiving end of the cache manager.
 */
class MockCachePlugin : public CachePlugin {
 public:
  static const unsigned kMockCacheSize;
  static const unsigned kMockListingNitems;

  MockCachePlugin(const string &socket_path, bool read_only)
      : CachePlugin(read_only ? (cvmfs::CAP_ALL_V1 & ~cvmfs::CAP_WRITE)
                              : cvmfs::CAP_ALL_V2) {
    bool retval = Listen("unix=" + socket_path);
    assert(retval);
    ProcessRequests(0);
    known_object.algorithm = shash::kSha1;
    known_object_content = "Hello, World";
    shash::HashString(known_object_content, &known_object);
    known_object_refcnt = 0;
    next_status = -1;
    listing_nitems = 0;
    listing_type = cvmfs::OBJECT_REGULAR;
    last_id = 0;
    last_reponame = NULL;
    last_client_instance = NULL;
  }

  virtual ~MockCachePlugin() { }

  void GetSession(uint64_t *id, char **reponame, char **client_instance) {
    SessionCtx *session_ctx = SessionCtx::GetInstance();
    assert(session_ctx);
    session_ctx->Get(id, reponame, client_instance);
  }

  string known_object_content;
  shash::Any known_object;
  shash::Any new_object;
  string new_object_content;
  int known_object_refcnt;
  int next_status;
  unsigned listing_nitems;
  cvmfs::EnumObjectType listing_type;
  uint64_t last_id;
  char *last_reponame;
  char *last_client_instance;
  std::map<std::string, manifest::Breadcrumb> breadcrumbs;

 protected:
  virtual cvmfs::EnumStatus ChangeRefcount(const shash::Any &id,
                                           int32_t change_by) {
    GetSession(&last_id, &last_reponame, &last_client_instance);
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

  virtual cvmfs::EnumStatus GetObjectInfo(const shash::Any &id,
                                          ObjectInfo *info) {
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

  virtual cvmfs::EnumStatus Pread(const shash::Any &id,
                                  uint64_t offset,
                                  uint32_t *size,
                                  unsigned char *buffer) {
    GetSession(&last_id, &last_reponame, &last_client_instance);
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

  virtual cvmfs::EnumStatus StartTxn(const shash::Any &id,
                                     const uint64_t txn_id,
                                     const ObjectInfo &info) {
    new_object = id;
    new_object_content.clear();
    return cvmfs::STATUS_OK;
  }

  virtual cvmfs::EnumStatus WriteTxn(const uint64_t txn_id,
                                     unsigned char *buffer,
                                     uint32_t size) {
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

  virtual cvmfs::EnumStatus GetInfo(Info *info) {
    info->size_bytes = kMockCacheSize;
    info->used_bytes = known_object_content.length();
    info->pinned_bytes = (known_object_refcnt == 0) ? 0 : info->used_bytes;
    info->no_shrink = 0;
    return cvmfs::STATUS_OK;
  }

  virtual cvmfs::EnumStatus Shrink(uint64_t shrink_to, uint64_t *used_bytes) {
    return (known_object_refcnt == 0) ? cvmfs::STATUS_OK
                                      : cvmfs::STATUS_PARTIAL;
  }

  virtual cvmfs::EnumStatus ListingBegin(uint64_t lst_id,
                                         cvmfs::EnumObjectType type) {
    listing_nitems = 0;
    listing_type = type;
    return cvmfs::STATUS_OK;
  }

  virtual cvmfs::EnumStatus ListingNext(int64_t lst_id, ObjectInfo *item) {
    if ((listing_type != cvmfs::OBJECT_REGULAR)
        || (listing_nitems >= kMockListingNitems))
      return cvmfs::STATUS_OUTOFBOUNDS;
    item->id = known_object;
    item->size = known_object_content.length();
    item->object_type = cvmfs::OBJECT_REGULAR;
    item->pinned = known_object_refcnt > 0;
    item->description = "/known_object";
    listing_nitems++;
    return cvmfs::STATUS_OK;
  }

  virtual cvmfs::EnumStatus ListingEnd(int64_t lst_id) {
    return cvmfs::STATUS_OK;
  }

  virtual cvmfs::EnumStatus LoadBreadcrumb(const std::string &fqrn,
                                           manifest::Breadcrumb *breadcrumb) {
    map<std::string, manifest::Breadcrumb>::const_iterator
        itr = breadcrumbs.find(fqrn);
    if (itr == breadcrumbs.end())
      return cvmfs::STATUS_NOENTRY;
    *breadcrumb = itr->second;
    return cvmfs::STATUS_OK;
  }

  virtual cvmfs::EnumStatus StoreBreadcrumb(
      const std::string &fqrn, const manifest::Breadcrumb &breadcrumb) {
    breadcrumbs[fqrn] = breadcrumb;
    return cvmfs::STATUS_OK;
  }
};

const unsigned MockCachePlugin::kMockCacheSize = 10 * 1024 * 1024;
const unsigned MockCachePlugin::kMockListingNitems = 100000;


class T_ExternalCacheManager : public ::testing::Test {
 protected:
  virtual void SetUp() {
    socket_path_ = "cvmfs_cache_plugin.socket";
    mock_plugin_ = new MockCachePlugin(socket_path_, false);

    fd_client = ConnectSocket(socket_path_);
    ASSERT_GE(fd_client, 0);
    cache_mgr_ = ExternalCacheManager::Create(fd_client, nfiles,
                                              "test:instance");
    ASSERT_TRUE(cache_mgr_ != NULL);
    quota_mgr_ = ExternalQuotaManager::Create(cache_mgr_);
    ASSERT_TRUE(cache_mgr_ != NULL);
    cache_mgr_->AcquireQuotaManager(quota_mgr_);
  }

  virtual void TearDown() {
    delete cache_mgr_;
    unlink(socket_path_.c_str());
    delete mock_plugin_;
  }

  CacheManager::LabeledObject LabelWithPath(const shash::Any &id,
                                            const std::string &path) {
    CacheManager::Label label;
    label.path = path;
    return CacheManager::LabeledObject(id, label);
  }

  static const unsigned nfiles;
  int fd_client;
  string socket_path_;
  MockCachePlugin *mock_plugin_;
  ExternalCacheManager *cache_mgr_;
  ExternalQuotaManager *quota_mgr_;
};

const unsigned T_ExternalCacheManager::nfiles = 128;


TEST_F(T_ExternalCacheManager, Connection) {
  EXPECT_GE(cache_mgr_->session_id(), 0);
  EXPECT_EQ(getpid(), cache_mgr_->quota_mgr()->GetPid());

  // Invalid query for session information outside callback
  uint64_t id;
  char *reponame;
  char *client_instance;
  mock_plugin_->GetSession(&id, &reponame, &client_instance);
  EXPECT_EQ(0U, id);
  EXPECT_EQ(NULL, reponame);
  EXPECT_EQ(NULL, client_instance);
}


TEST_F(T_ExternalCacheManager, OpenClose) {
  EXPECT_EQ(-EBADF, cache_mgr_->Close(0));
  shash::Any rnd_id(shash::kSha1);
  rnd_id.Randomize();
  EXPECT_EQ(-ENOENT, cache_mgr_->Open(CacheManager::LabeledObject(rnd_id)));
  uint64_t session_id = mock_plugin_->last_id;
  EXPECT_EQ(0, strcmp(mock_plugin_->last_reponame, "test"));
  EXPECT_EQ(0, strcmp(mock_plugin_->last_client_instance, "instance"));

  int fds[nfiles];
  for (unsigned i = 0; i < nfiles; ++i) {
    fds[i] = cache_mgr_->Open(
        CacheManager::LabeledObject(mock_plugin_->known_object));
    EXPECT_GE(fds[i], 0);
  }
  EXPECT_EQ(session_id, mock_plugin_->last_id);
  EXPECT_EQ(static_cast<int>(nfiles), mock_plugin_->known_object_refcnt);
  EXPECT_EQ(-ENFILE, cache_mgr_->Open(CacheManager::LabeledObject(
                         mock_plugin_->known_object)));
  for (unsigned i = 0; i < nfiles; ++i) {
    EXPECT_EQ(0, cache_mgr_->Close(fds[i]));
  }
  EXPECT_EQ(0, mock_plugin_->known_object_refcnt);

  mock_plugin_->next_status = cvmfs::STATUS_MALFORMED;
  EXPECT_EQ(-EINVAL, cache_mgr_->Open(CacheManager::LabeledObject(
                         mock_plugin_->known_object)));
  mock_plugin_->next_status = -1;
}


TEST_F(T_ExternalCacheManager, ReadOnly) {
  // Re-initialize as read-only
  delete cache_mgr_;
  unlink(socket_path_.c_str());
  delete mock_plugin_;
  mock_plugin_ = new MockCachePlugin(socket_path_, true);
  fd_client = ConnectSocket(socket_path_);
  ASSERT_GE(fd_client, 0);
  cache_mgr_ = ExternalCacheManager::Create(fd_client, nfiles, "test");
  ASSERT_TRUE(cache_mgr_ != NULL);
  quota_mgr_ = ExternalQuotaManager::Create(cache_mgr_);
  ASSERT_TRUE(cache_mgr_ != NULL);
  cache_mgr_->AcquireQuotaManager(quota_mgr_);
  EXPECT_GE(cache_mgr_->session_id(), 0);

  int fd = cache_mgr_->Open(
      CacheManager::LabeledObject(mock_plugin_->known_object));
  EXPECT_GE(fd, 0);
  EXPECT_EQ(0, cache_mgr_->Close(fd));

  shash::Any id(shash::kSha1);
  string content = "foo";
  HashString(content, &id);
  void *txn = alloca(cache_mgr_->SizeOfTxn());
  EXPECT_EQ(-EROFS, cache_mgr_->StartTxn(id, content.length(), txn));
  unsigned char *data = const_cast<unsigned char *>(
      reinterpret_cast<const unsigned char *>(content.data()));
  EXPECT_FALSE(cache_mgr_->CommitFromMem(LabelWithPath(id, "test"), data,
                                         content.length()));
}


TEST_F(T_ExternalCacheManager, GetSize) {
  EXPECT_EQ(-EBADF, cache_mgr_->GetSize(0));
  int fd = cache_mgr_->Open(
      CacheManager::LabeledObject(mock_plugin_->known_object));
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
  fds[0] = cache_mgr_->Open(
      CacheManager::LabeledObject(mock_plugin_->known_object));
  EXPECT_GE(fds[0], 0);
  for (unsigned i = 1; i < nfiles; ++i) {
    fds[i] = cache_mgr_->Dup(fds[0]);
    EXPECT_GE(fds[i], 0);
  }
  EXPECT_EQ(static_cast<int>(nfiles), mock_plugin_->known_object_refcnt);
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

  int fd = cache_mgr_->Open(
      CacheManager::LabeledObject(mock_plugin_->known_object));
  EXPECT_GE(fd, 0);
  EXPECT_EQ(-EINVAL, cache_mgr_->Pread(fd, buffer, 1, 64));
  int64_t len = cache_mgr_->Pread(fd, buffer, 64, 0);
  EXPECT_EQ(static_cast<int>(mock_plugin_->known_object_content.length()), len);
  EXPECT_EQ(mock_plugin_->known_object_content, string(buffer, len));
  EXPECT_EQ(1, cache_mgr_->Pread(fd, buffer, 1, len - 1));
  EXPECT_EQ(mock_plugin_->known_object_content[len - 1], buffer[0]);
  EXPECT_EQ(0, cache_mgr_->Close(fd));
}


TEST_F(T_ExternalCacheManager, Readahead) {
  EXPECT_EQ(-EBADF, cache_mgr_->Readahead(0));
  int fd = cache_mgr_->Open(
      CacheManager::LabeledObject(mock_plugin_->known_object));
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
  EXPECT_TRUE(cache_mgr_->CommitFromMem(LabelWithPath(id, "test"), data,
                                        content.length()));
  unsigned char *buffer;
  uint64_t size;
  EXPECT_TRUE(cache_mgr_->Open2Mem(LabelWithPath(id, "test"), &buffer, &size));
  EXPECT_EQ(content, string(reinterpret_cast<char *>(buffer), size));
  free(buffer);

  content = "";
  HashString(content, &id);
  data = NULL;
  EXPECT_TRUE(cache_mgr_->CommitFromMem(LabelWithPath(id, "test"), data,
                                        content.length()));
  EXPECT_TRUE(cache_mgr_->Open2Mem(LabelWithPath(id, "test"), &buffer, &size));
  EXPECT_EQ(0U, size);
  EXPECT_EQ(NULL, buffer);

  unsigned large_size = 50 * 1024 * 1024;
  unsigned char *large_buffer = reinterpret_cast<unsigned char *>(
      scalloc(large_size, 1));
  EXPECT_TRUE(cache_mgr_->CommitFromMem(LabelWithPath(id, "test"), large_buffer,
                                        large_size));
  unsigned char *large_buffer_verify = reinterpret_cast<unsigned char *>(
      smalloc(large_size));
  EXPECT_TRUE(cache_mgr_->Open2Mem(LabelWithPath(id, "test"),
                                   &large_buffer_verify, &size));
  EXPECT_EQ(large_size, size);
  EXPECT_EQ(0, memcmp(large_buffer, large_buffer_verify, large_size));
  free(large_buffer_verify);
  free(large_buffer);

  large_size = 50 * 1024 * 1024 + 1;
  large_buffer = reinterpret_cast<unsigned char *>(scalloc(large_size, 1));
  EXPECT_TRUE(cache_mgr_->CommitFromMem(LabelWithPath(id, "test"), large_buffer,
                                        large_size));
  large_buffer_verify = reinterpret_cast<unsigned char *>(smalloc(large_size));
  EXPECT_TRUE(cache_mgr_->Open2Mem(LabelWithPath(id, "test"),
                                   &large_buffer_verify, &size));
  EXPECT_EQ(large_size, size);
  EXPECT_EQ(0, memcmp(large_buffer, large_buffer_verify, large_size));
  free(large_buffer_verify);
  free(large_buffer);

  // test unordered upload of chunks (and failure inbetween)
}


TEST_F(T_ExternalCacheManager, TransactionAbort) {
  shash::Any id(shash::kSha1);
  uint64_t write_size = cache_mgr_->max_object_size_ * 4;
  unsigned char *write_buffer = static_cast<unsigned char *>(
      smalloc(write_size));
  memset(write_buffer, static_cast<unsigned char>(kMemMarker), write_size);
  HashMem(write_buffer, write_size, &id);

  void *txn = alloca(cache_mgr_->SizeOfTxn());
  EXPECT_EQ(0, cache_mgr_->StartTxn(id, write_size, txn));
  EXPECT_EQ(0, cache_mgr_->Reset(txn));
  EXPECT_EQ(2, cache_mgr_->Write(write_buffer, 2, txn));
  EXPECT_EQ(0, cache_mgr_->Reset(txn));
  EXPECT_EQ(static_cast<int>(write_size / 2),
            cache_mgr_->Write(write_buffer, write_size / 2, txn));
  EXPECT_EQ(0, cache_mgr_->Reset(txn));
  EXPECT_EQ(static_cast<int>(write_size),
            cache_mgr_->Write(write_buffer, write_size, txn));
  EXPECT_EQ(0, cache_mgr_->CommitTxn(txn));

  uint64_t read_size = write_size;
  unsigned char *read_buffer = static_cast<unsigned char *>(smalloc(read_size));
  EXPECT_TRUE(cache_mgr_->Open2Mem(LabelWithPath(id, "test"), &read_buffer,
                                   &read_size));
  EXPECT_EQ(read_size, write_size);
  EXPECT_EQ(0, memcmp(read_buffer, write_buffer, read_size));
  free(read_buffer);
  free(write_buffer);
}


namespace {

struct BackchannelData {
  BackchannelData() : nfired(0) { }
  unsigned nfired;
  int channel[2];
};

void *MainBackchannel(void *data) {
  BackchannelData *bd = reinterpret_cast<BackchannelData *>(data);
  while (true) {
    char buf;
    ReadPipe(bd->channel[0], &buf, 1);
    if (buf == 'R') {
      bd->nfired++;
      continue;
    }
    if (buf == 'T')
      return NULL;
    abort();
  }
}

}  // anonymous namespace

TEST_F(T_ExternalCacheManager, Detach) {
  int fd = cache_mgr_->Open(
      CacheManager::LabeledObject(mock_plugin_->known_object));
  EXPECT_GE(fd, 0);
  BackchannelData bd;
  quota_mgr_->RegisterBackChannel(bd.channel, "xyz");
  pthread_t thread_backchannel;
  int retval = pthread_create(&thread_backchannel, NULL, MainBackchannel, &bd);
  ASSERT_EQ(0, retval);

  mock_plugin_->AskToDetach();

  unsigned size = 64;
  char buf[size];
  EXPECT_EQ(static_cast<int>(mock_plugin_->known_object_content.length()),
            cache_mgr_->Pread(fd, buf, size, 0));
  EXPECT_EQ(mock_plugin_->known_object_content,
            string(buf, mock_plugin_->known_object_content.length()));
  EXPECT_EQ(0, cache_mgr_->Close(fd));

  // Not picked up anymore by single threaded cache manager
  mock_plugin_->AskToDetach();

  buf[0] = 'T';
  WritePipe(bd.channel[1], &buf[0], 1);
  pthread_join(thread_backchannel, NULL);
  quota_mgr_->UnregisterBackChannel(bd.channel, "xyz");
  EXPECT_EQ(1U, bd.nfired);
}


TEST_F(T_ExternalCacheManager, Info) {
  EXPECT_EQ(MockCachePlugin::kMockCacheSize, quota_mgr_->GetCapacity());
  EXPECT_EQ(mock_plugin_->known_object_content.length(), quota_mgr_->GetSize());
  EXPECT_EQ(0U, quota_mgr_->GetSizePinned());

  int fd = cache_mgr_->Open(
      CacheManager::LabeledObject(mock_plugin_->known_object));
  EXPECT_GE(fd, 0);
  EXPECT_EQ(mock_plugin_->known_object_content.length(),
            quota_mgr_->GetSizePinned());
  EXPECT_EQ(0, cache_mgr_->Close(fd));
}


TEST_F(T_ExternalCacheManager, Shrink) {
  EXPECT_TRUE(quota_mgr_->Cleanup(0));
  int fd = cache_mgr_->Open(
      CacheManager::LabeledObject(mock_plugin_->known_object));
  EXPECT_GE(fd, 0);
  EXPECT_FALSE(quota_mgr_->Cleanup(0));
  EXPECT_EQ(0, cache_mgr_->Close(fd));
}


TEST_F(T_ExternalCacheManager, Listing) {
  vector<string> expected_listing;
  for (unsigned i = 0; i < MockCachePlugin::kMockListingNitems; ++i)
    expected_listing.push_back("/known_object");
  vector<string> listing = quota_mgr_->List();
  EXPECT_EQ(expected_listing.size(), listing.size());
  EXPECT_EQ(expected_listing, listing);

  vector<string> empty;
  EXPECT_EQ(0, mock_plugin_->known_object_refcnt);
  EXPECT_EQ(empty, quota_mgr_->ListCatalogs());
  EXPECT_EQ(empty, quota_mgr_->ListVolatile());
  EXPECT_EQ(empty, quota_mgr_->ListPinned());
}

TEST_F(T_ExternalCacheManager, Breadcrumbs) {
  manifest::Breadcrumb breadcrumb;
  breadcrumb = cache_mgr_->LoadBreadcrumb("test");
  EXPECT_FALSE(breadcrumb.IsValid());

  shash::Any hash(shash::kShake128);
  hash.Randomize();
  manifest::Manifest manifest(hash, 1, "");
  manifest.set_repository_name("test");
  manifest.set_publish_timestamp(1);
  EXPECT_TRUE(cache_mgr_->StoreBreadcrumb(manifest));

  breadcrumb = cache_mgr_->LoadBreadcrumb("test");
  EXPECT_TRUE(breadcrumb.IsValid());
  EXPECT_EQ(hash, breadcrumb.catalog_hash);
  EXPECT_EQ(1U, breadcrumb.timestamp);
}

namespace {

struct ThreadData {
  ExternalCacheManager *cache_mgr;
  MockCachePlugin *mock_plugin;
  unsigned large_size;
  unsigned char *large_buffer;
  shash::Any id;
};

static void *MainMultiThread(void *data) {
  ThreadData *td = reinterpret_cast<ThreadData *>(data);

  uint64_t size;
  unsigned char *buffer;
  CacheManager::Label label;
  label.path = "test";
  EXPECT_TRUE(td->cache_mgr->Open2Mem(
      CacheManager::LabeledObject(td->id, label), &buffer, &size));
  EXPECT_EQ(td->large_size, size);
  EXPECT_EQ(0, memcmp(buffer, td->large_buffer, size));
  free(buffer);
  return NULL;
}

static void *MainDetach(void *data) {
  ThreadData *td = reinterpret_cast<ThreadData *>(data);

  for (unsigned i = 0; i < 1000; ++i) {
    td->mock_plugin->AskToDetach();
  }
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
  EXPECT_TRUE(cache_mgr_->CommitFromMem(LabelWithPath(id, "test"), large_buffer,
                                        large_size));

  const unsigned num_threads = 10;
  pthread_t threads[num_threads];
  ThreadData td[num_threads];
  for (unsigned i = 0; i < num_threads; ++i) {
    td[i].cache_mgr = cache_mgr_;
    td[i].mock_plugin = mock_plugin_;
    td[i].large_size = large_size;
    td[i].large_buffer = large_buffer;
    td[i].id = id;

    if (i == num_threads - 1) {
      int retval = pthread_create(&threads[i], NULL, MainDetach, &td[i]);
      assert(retval == 0);
    } else {
      int retval = pthread_create(&threads[i], NULL, MainMultiThread, &td[i]);
      assert(retval == 0);
    }
  }
  // TODO(jblomer): Test info and listing calls multithreaded
  for (unsigned i = 0; i < num_threads; ++i) {
    pthread_join(threads[i], NULL);
  }

  free(large_buffer);
}


TEST_F(T_ExternalCacheManager, SaveState) {
  // Should not crash
  void *data = cache_mgr_->SaveState(-1);
  cache_mgr_->RestoreState(-1, data);
  cache_mgr_->FreeState(-1, data);

  // Now with a new cache manager
  int fd = cache_mgr_->Open(
      CacheManager::LabeledObject(mock_plugin_->known_object));
  uint64_t old_session_id = mock_plugin_->last_id;
  EXPECT_GE(fd, 0);
  data = cache_mgr_->SaveState(-1);
  delete cache_mgr_;
  fd_client = ConnectSocket(socket_path_);
  ASSERT_GE(fd_client, 0);
  cache_mgr_ = ExternalCacheManager::Create(fd_client, nfiles, "test");
  ASSERT_TRUE(cache_mgr_ != NULL);
  quota_mgr_ = ExternalQuotaManager::Create(cache_mgr_);
  ASSERT_TRUE(cache_mgr_ != NULL);
  cache_mgr_->AcquireQuotaManager(quota_mgr_);
  cache_mgr_->RestoreState(-1, data);
  cache_mgr_->FreeState(-1, data);
  char buffer[64];
  int64_t len = cache_mgr_->Pread(fd, buffer, 64, 0);
  EXPECT_EQ(static_cast<int>(mock_plugin_->known_object_content.length()), len);
  EXPECT_EQ(mock_plugin_->known_object_content, string(buffer, len));
  EXPECT_EQ(0, cache_mgr_->Close(fd));

  EXPECT_NE(old_session_id, mock_plugin_->last_id);
  EXPECT_EQ(0, strcmp(mock_plugin_->last_reponame, "test"));
  EXPECT_EQ(NULL, mock_plugin_->last_client_instance);
}
