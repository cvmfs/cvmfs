/**
 * This file is part of the CernVM File System.
 *
 * Black box unit test for external cache managers.  The external cache manager
 * must be writable, otherwise it cannot be tested with this suite.
 */

#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <set>
#include <string>
#include <vector>

#include "cache.pb.h"
#include "cache_extern.h"
#include "crypto/hash.h"
#include "globals.h"
#include "util/posix.h"
#include "util/prng.h"
#include "util/string.h"

using namespace std;  // NOLINT


class T_CachePlugin : public ::testing::Test {
 protected:
  virtual void SetUp() {
    int fd_client = Connect();
    ASSERT_GE(fd_client, 0);
    cache_mgr_ = ExternalCacheManager::Create(fd_client, nfiles, "test");
    ASSERT_TRUE(cache_mgr_ != NULL);
    quota_mgr_ = ExternalQuotaManager::Create(cache_mgr_);
    ASSERT_TRUE(cache_mgr_ != NULL);
    cache_mgr_->AcquireQuotaManager(quota_mgr_);
  }

  virtual void TearDown() { delete cache_mgr_; }

  int Connect() {
    vector<string> tokens = SplitString(g_plugin_locator, '=');
    if (tokens[0] == "unix") {
      return ConnectSocket(tokens[1]);
    } else if (tokens[0] == "tcp") {
      vector<string> tcp_address = SplitString(tokens[1], ':');
      if (tcp_address.size() != 2) {
        printf("invalid locator: %s\n", g_plugin_locator.c_str());
        abort();
      }
      return ConnectTcpEndpoint(tcp_address[0], String2Uint64(tcp_address[1]));
    } else {
      printf("invalid locator: %s\n", g_plugin_locator.c_str());
      abort();
    }
  }

  CacheManager::LabeledObject LabelWithPath(const shash::Any &id,
                                            const std::string &path) {
    CacheManager::Label label;
    label.path = path;
    return CacheManager::LabeledObject(id, label);
  }

  static const int nfiles;
  ExternalCacheManager *cache_mgr_;
  ExternalQuotaManager *quota_mgr_;
};

const int T_CachePlugin::nfiles = 128;


TEST_F(T_CachePlugin, Connection) {
  EXPECT_GE(cache_mgr_->session_id(), 0);

  int fd_second = Connect();
  ASSERT_GE(fd_second, 0);
  ExternalCacheManager *cache_mgr_second = ExternalCacheManager::Create(
      fd_second, nfiles, "test 2nd");
  ASSERT_TRUE(cache_mgr_second != NULL);
  EXPECT_GE(cache_mgr_second->session_id(), 0);

  EXPECT_NE(cache_mgr_second->session_id(), cache_mgr_->session_id());
}


TEST_F(T_CachePlugin, OpenClose) {
  shash::Any rnd_id(shash::kSha1);
  rnd_id.Randomize();
  EXPECT_EQ(-ENOENT, cache_mgr_->Open(CacheManager::LabeledObject(rnd_id)));

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
}


TEST_F(T_CachePlugin, StoreEmpty) {
  shash::Any empty_id(shash::kSha1);
  string empty;
  shash::HashString(empty, &empty_id);
  EXPECT_TRUE(
      cache_mgr_->CommitFromMem(LabelWithPath(empty_id, "enpty"), NULL, 0));

  unsigned char *buffer;
  uint64_t size;
  EXPECT_TRUE(
      cache_mgr_->Open2Mem(LabelWithPath(empty_id, "test"), &buffer, &size));
  EXPECT_EQ(0U, size);
  EXPECT_EQ(NULL, buffer);
  free(buffer);

  int fd = cache_mgr_->Open(CacheManager::LabeledObject(empty_id));
  EXPECT_GE(fd, 0);
  unsigned char small_buf[1];
  EXPECT_EQ(0, cache_mgr_->Pread(fd, small_buf, 1, 0));
  EXPECT_EQ(-EINVAL, cache_mgr_->Pread(fd, small_buf, 1, 1));
  EXPECT_EQ(0, cache_mgr_->Close(fd));
}


TEST_F(T_CachePlugin, HashAlgorithms) {
  shash::Any id_sha1(shash::kSha1);
  shash::Any id_rmd160(shash::kRmd160);
  shash::Any id_shake128(shash::kShake128);
  string content = "foo";
  HashString(content, &id_sha1);
  HashString(content, &id_rmd160);
  HashString(content, &id_shake128);
  unsigned char *data = const_cast<unsigned char *>(
      reinterpret_cast<const unsigned char *>(content.data()));
  EXPECT_TRUE(cache_mgr_->CommitFromMem(LabelWithPath(id_sha1, "sha1"), data,
                                        content.length()));
  EXPECT_TRUE(cache_mgr_->CommitFromMem(LabelWithPath(id_rmd160, "rmd160"),
                                        data, content.length()));
  EXPECT_TRUE(cache_mgr_->CommitFromMem(LabelWithPath(id_shake128, "shake128"),
                                        data, content.length()));
  unsigned char *buffer;
  uint64_t size;
  EXPECT_TRUE(
      cache_mgr_->Open2Mem(LabelWithPath(id_sha1, "sha1"), &buffer, &size));
  EXPECT_EQ(content, string(reinterpret_cast<char *>(buffer), size));
  free(buffer);
  EXPECT_TRUE(
      cache_mgr_->Open2Mem(LabelWithPath(id_rmd160, "rmd160"), &buffer, &size));
  EXPECT_EQ(content, string(reinterpret_cast<char *>(buffer), size));
  free(buffer);
  EXPECT_TRUE(cache_mgr_->Open2Mem(LabelWithPath(id_shake128, "id_shake128"),
                                   &buffer, &size));
  EXPECT_EQ(content, string(reinterpret_cast<char *>(buffer), size));
  free(buffer);
}


TEST_F(T_CachePlugin, Read) {
  unsigned size_even = 24 * 1024 * 1024;
  unsigned size_odd = 24 * 1024 * 1024 + 1;
  unsigned char *buffer = reinterpret_cast<unsigned char *>(
      scalloc(size_odd, 1));
  memset(buffer, 1, size_odd);
  shash::Any id_even(shash::kSha1);
  shash::Any id_odd(shash::kSha1);
  shash::HashMem(buffer, size_even, &id_even);
  shash::HashMem(buffer, size_odd, &id_odd);
  EXPECT_TRUE(cache_mgr_->CommitFromMem(LabelWithPath(id_even, "even"), buffer,
                                        size_even));
  EXPECT_TRUE(cache_mgr_->CommitFromMem(LabelWithPath(id_odd, "odd"), buffer,
                                        size_odd));

  unsigned char *read_buffer;
  uint64_t size;
  EXPECT_TRUE(cache_mgr_->Open2Mem(LabelWithPath(id_even, "even"), &read_buffer,
                                   &size));
  EXPECT_EQ(size, size_even);
  EXPECT_EQ(0, memcmp(read_buffer, buffer, size_even));
  free(read_buffer);
  EXPECT_TRUE(
      cache_mgr_->Open2Mem(LabelWithPath(id_odd, "odd"), &read_buffer, &size));
  EXPECT_EQ(size, size_odd);
  EXPECT_EQ(0, memcmp(read_buffer, buffer, size_odd));
  free(read_buffer);

  int fd = cache_mgr_->Open(CacheManager::LabeledObject(id_odd));
  EXPECT_GE(fd, 0);
  EXPECT_EQ(0, cache_mgr_->Pread(fd, NULL, 0, 0));

  read_buffer = reinterpret_cast<unsigned char *>(
      smalloc(2 * cache_mgr_->max_object_size()));
  EXPECT_EQ(0, cache_mgr_->Pread(fd, read_buffer, 0, size_odd));
  EXPECT_EQ(-EINVAL, cache_mgr_->Pread(fd, read_buffer, 1, size_odd + 1));
  EXPECT_EQ(1, cache_mgr_->Pread(fd, read_buffer, 1, size_odd - 1));
  EXPECT_EQ(1, read_buffer[0]);

  uint64_t total_size = 0;
  Prng prng;
  prng.InitLocaltime();
  while (total_size < size_odd) {
    int32_t next_size = prng.Next(2 * cache_mgr_->max_object_size() - 1) + 1;
    int bytes_read = cache_mgr_->Pread(fd, read_buffer, next_size, total_size);
    EXPECT_GT(bytes_read, 0);
    EXPECT_EQ(0, memcmp(read_buffer, buffer, bytes_read));
    total_size += bytes_read;
    EXPECT_TRUE((bytes_read == next_size) || (total_size == size_odd))
        << next_size << " bytes requested, " << bytes_read
        << " bytes received, " << "read so far: " << total_size << "/"
        << size_odd << " bytes";
  }

  EXPECT_EQ(0, cache_mgr_->Close(fd));
  free(read_buffer);
}


TEST_F(T_CachePlugin, TransactionAbort) {
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
  EXPECT_TRUE(cache_mgr_->Open2Mem(LabelWithPath(id, "test"), &buf, &size));
  EXPECT_EQ(content, string(reinterpret_cast<char *>(buf), size));
  free(buf);
}


TEST_F(T_CachePlugin, CommitHandover) {
  shash::Any id(shash::kSha1);
  string content = "handover";
  HashString(content, &id);
  void *txn = alloca(cache_mgr_->SizeOfTxn());
  EXPECT_EQ(0, cache_mgr_->StartTxn(id, content.length(), txn));
  EXPECT_EQ(static_cast<int>(content.length()),
            cache_mgr_->Write(content.data(), content.length(), txn));
  int fd = cache_mgr_->OpenFromTxn(txn);
  EXPECT_GE(fd, 0);
  EXPECT_EQ(static_cast<int>(content.length()), cache_mgr_->GetSize(fd));
  char char_buffer[64];
  EXPECT_EQ(static_cast<int>(content.length()),
            cache_mgr_->Pread(fd, char_buffer, 64, 0));
  EXPECT_EQ(content, string(char_buffer, content.length()));
  EXPECT_EQ(0, cache_mgr_->CommitTxn(txn));
  EXPECT_EQ(0, cache_mgr_->Close(fd));

  unsigned char *buf;
  uint64_t size;
  EXPECT_TRUE(cache_mgr_->Open2Mem(LabelWithPath(id, "test"), &buf, &size));
  EXPECT_EQ(content, string(reinterpret_cast<char *>(buf), size));
  free(buf);
}


TEST_F(T_CachePlugin, CommitConcurrent) {
  shash::Any id(shash::kSha1);
  string content = "concurrent";
  HashString(content, &id);

  void *txn1 = alloca(cache_mgr_->SizeOfTxn());
  void *txn2 = alloca(cache_mgr_->SizeOfTxn());
  EXPECT_EQ(0, cache_mgr_->StartTxn(id, content.length(), txn1));
  EXPECT_EQ(0, cache_mgr_->StartTxn(id, content.length(), txn2));
  EXPECT_EQ(static_cast<int>(content.length()),
            cache_mgr_->Write(content.data(), content.length(), txn1));
  EXPECT_EQ(static_cast<int>(content.length()),
            cache_mgr_->Write(content.data(), content.length(), txn2));
  EXPECT_EQ(0, cache_mgr_->CommitTxn(txn1));
  EXPECT_EQ(0, cache_mgr_->CommitTxn(txn2));

  unsigned char *buf;
  uint64_t size;
  EXPECT_TRUE(cache_mgr_->Open2Mem(LabelWithPath(id, "test"), &buf, &size));
  EXPECT_EQ(content, string(reinterpret_cast<char *>(buf), size));
  free(buf);
}


TEST_F(T_CachePlugin, Info) {
  if (!(cache_mgr_->capabilities() & cvmfs::CAP_INFO)) {
    printf("Skipping\n");
    return;
  }

  EXPECT_GT(quota_mgr_->GetCapacity(), 0U);
  EXPECT_GE(quota_mgr_->GetCapacity(), quota_mgr_->GetSize());
  EXPECT_GE(quota_mgr_->GetSize(), quota_mgr_->GetSizePinned());

  unsigned size_pinned = quota_mgr_->GetSizePinned();
  shash::Any id(shash::kSha1);
  string content = "foo";
  HashString(content, &id);
  unsigned char *data = const_cast<unsigned char *>(
      reinterpret_cast<const unsigned char *>(content.data()));
  EXPECT_TRUE(cache_mgr_->CommitFromMem(LabelWithPath(id, "test"), data,
                                        content.length()));
  int fd = cache_mgr_->Open(CacheManager::LabeledObject(id));
  EXPECT_GE(fd, 0);
  EXPECT_GT(quota_mgr_->GetSizePinned(), size_pinned);
  EXPECT_EQ(0, cache_mgr_->Close(fd));
}


TEST_F(T_CachePlugin, Shrink) {
  if (!(cache_mgr_->capabilities() & cvmfs::CAP_SHRINK)) {
    printf("Skipping\n");
    return;
  }

  EXPECT_TRUE(quota_mgr_->Cleanup(0));
  EXPECT_TRUE(quota_mgr_->Cleanup(0));
  uint64_t size_vanilla = quota_mgr_->GetSize();
  shash::Any id_vol(shash::kSha1);
  shash::Any id_reg(shash::kSha1);
  shash::Any id_clg(shash::kSha1);
  shash::Any id_txn(shash::kSha1);
  string str_vol = "volatile";
  string str_reg = "regular";
  string str_clg = "catalog";
  string str_txn = "transaction";
  HashString(str_vol, &id_vol);
  HashString(str_reg, &id_reg);
  HashString(str_clg, &id_clg);
  HashString(str_txn, &id_txn);
  unsigned char *dat_vol = const_cast<unsigned char *>(
      reinterpret_cast<const unsigned char *>(str_vol.data()));
  unsigned char *dat_reg = const_cast<unsigned char *>(
      reinterpret_cast<const unsigned char *>(str_reg.data()));
  unsigned char *dat_clg = const_cast<unsigned char *>(
      reinterpret_cast<const unsigned char *>(str_clg.data()));
  EXPECT_TRUE(cache_mgr_->CommitFromMem(LabelWithPath(id_vol, ""), dat_vol,
                                        str_vol.length()));
  uint64_t size_with1 = quota_mgr_->GetSize();
  EXPECT_GT(size_with1, size_vanilla);
  EXPECT_TRUE(cache_mgr_->CommitFromMem(LabelWithPath(id_reg, ""), dat_reg,
                                        str_reg.length()));
  uint64_t size_with2 = quota_mgr_->GetSize();
  EXPECT_GT(size_with2, size_with1);
  EXPECT_TRUE(cache_mgr_->CommitFromMem(LabelWithPath(id_clg, ""), dat_clg,
                                        str_clg.length()));
  uint64_t size_with3 = quota_mgr_->GetSize();
  EXPECT_GT(size_with3, size_with2);
  void *txn = alloca(cache_mgr_->SizeOfTxn());
  EXPECT_EQ(0, cache_mgr_->StartTxn(id_txn, str_txn.length(), txn));
  EXPECT_EQ(static_cast<int>(str_txn.length()),
            cache_mgr_->Write(str_txn.data(), str_txn.length(), txn));
  uint64_t size_with4 = quota_mgr_->GetSize();
  EXPECT_GE(size_with4, size_with3);

  int fd_clg = cache_mgr_->Open(CacheManager::LabeledObject(id_clg));
  int fd_vol = cache_mgr_->Open(CacheManager::LabeledObject(id_vol));
  EXPECT_GE(fd_clg, 0);
  EXPECT_GE(fd_vol, 0);
  EXPECT_FALSE(quota_mgr_->Cleanup(0));
  EXPECT_EQ(-ENOENT, cache_mgr_->Open(CacheManager::LabeledObject(id_reg)));
  EXPECT_EQ(0, cache_mgr_->Close(fd_clg));
  EXPECT_EQ(0, cache_mgr_->Close(fd_vol));
  EXPECT_TRUE(quota_mgr_->Cleanup(0));
  EXPECT_EQ(-ENOENT, cache_mgr_->Open(CacheManager::LabeledObject(id_vol)));
  EXPECT_EQ(-ENOENT, cache_mgr_->Open(CacheManager::LabeledObject(id_clg)));

  EXPECT_EQ(0, cache_mgr_->CommitTxn(txn));
  unsigned char *buf;
  uint64_t size;
  EXPECT_TRUE(cache_mgr_->Open2Mem(LabelWithPath(id_txn, "test"), &buf, &size));
  EXPECT_EQ(str_txn, string(reinterpret_cast<char *>(buf), size));
  free(buf);
}


TEST_F(T_CachePlugin, List) {
  if (!(cache_mgr_->capabilities() & cvmfs::CAP_LIST)) {
    printf("Skipping\n");
    return;
  }

  set<int> open_fds;
  set<string> descriptions;
  unsigned N = 1000;
  for (unsigned i = 0; i < N; ++i) {
    shash::Any id(shash::kSha1);
    unsigned char *data = reinterpret_cast<unsigned char *>(&i);
    HashMem(data, sizeof(i), &id);
    descriptions.insert(id.ToString());
    EXPECT_TRUE(cache_mgr_->CommitFromMem(LabelWithPath(id, id.ToString()),
                                          data, sizeof(i)));
    if ((i % 10) == 0) {
      int fd = cache_mgr_->Open(CacheManager::LabeledObject(id));
      EXPECT_GE(fd, 0);
      open_fds.insert(fd);
    }
  }

  if ((cache_mgr_->capabilities() & cvmfs::CAP_REFCOUNT)) {
    vector<string> list_pinned = quota_mgr_->ListPinned();
    EXPECT_EQ(open_fds.size(), list_pinned.size());
    for (unsigned i = 0; i < list_pinned.size(); ++i) {
      descriptions.erase(list_pinned[i]);
    }
    EXPECT_EQ(N - list_pinned.size(), descriptions.size());
  }

  vector<string> list = quota_mgr_->List();
  for (unsigned i = 0; i < list.size(); ++i) {
    descriptions.erase(list[i]);
  }
  EXPECT_TRUE(descriptions.empty());
  for (set<int>::const_iterator i = open_fds.begin(), i_end = open_fds.end();
       i != i_end; ++i) {
    EXPECT_EQ(0, cache_mgr_->Close(*i));
  }
}


TEST_F(T_CachePlugin, Breadcrumbs) {
  if (!(cache_mgr_->capabilities() & cvmfs::CAP_BREADCRUMB)) {
    printf("Skipping\n");
    return;
  }

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
