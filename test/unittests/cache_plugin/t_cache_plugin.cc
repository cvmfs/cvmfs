/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <cstring>

#include "cache.pb.h"
#include "cache_extern.h"
#include "globals.h"
#include "hash.h"
#include "prng.h"
#include "util/posix.h"

using namespace std;  // NOLINT


class T_CachePlugin : public ::testing::Test {
 protected:
  virtual void SetUp() {
    int fd_client = ConnectSocket(g_plugin_locator);
    ASSERT_GE(fd_client, 0);
    cache_mgr_ = ExternalCacheManager::Create(fd_client, nfiles);
    ASSERT_TRUE(cache_mgr_ != NULL);
  }

  virtual void TearDown() {
    delete cache_mgr_;
  }

  static const int nfiles;
  ExternalCacheManager *cache_mgr_;
};

const int T_CachePlugin::nfiles = 128;


TEST_F(T_CachePlugin, Connection) {
  EXPECT_GE(cache_mgr_->session_id(), 0);

  int fd_second = ConnectSocket(g_plugin_locator);
  ASSERT_GE(fd_second, 0);
  ExternalCacheManager *cache_mgr_second =
    ExternalCacheManager::Create(fd_second, nfiles);
  ASSERT_TRUE(cache_mgr_second != NULL);
  EXPECT_GE(cache_mgr_second->session_id(), 0);

  EXPECT_NE(cache_mgr_second->session_id(), cache_mgr_->session_id());
}


TEST_F(T_CachePlugin, OpenClose) {
  shash::Any rnd_id(shash::kSha1);
  rnd_id.Randomize();
  EXPECT_EQ(-ENOENT, cache_mgr_->Open(CacheManager::Bless(rnd_id)));

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
}


TEST_F(T_CachePlugin, StoreEmpty) {
  shash::Any empty_id(shash::kSha1);
  string empty;
  shash::HashString(empty, &empty_id);
  EXPECT_TRUE(cache_mgr_->CommitFromMem(empty_id, NULL, 0, "empty"));

  unsigned char *buffer;
  uint64_t size;
  EXPECT_TRUE(cache_mgr_->Open2Mem(empty_id, "test", &buffer, &size));
  EXPECT_EQ(0U, size);
  EXPECT_EQ(NULL, buffer);
  free(buffer);
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
  EXPECT_TRUE(
    cache_mgr_->CommitFromMem(id_sha1, data, content.length(), "sha1"));
  EXPECT_TRUE(
    cache_mgr_->CommitFromMem(id_rmd160, data, content.length(), "rmd160"));
  EXPECT_TRUE(
    cache_mgr_->CommitFromMem(id_shake128, data, content.length(), "shake128"));
  unsigned char *buffer;
  uint64_t size;
  EXPECT_TRUE(cache_mgr_->Open2Mem(id_sha1, "sha1", &buffer, &size));
  EXPECT_EQ(content, string(reinterpret_cast<char *>(buffer), size));
  free(buffer);
  EXPECT_TRUE(cache_mgr_->Open2Mem(id_rmd160, "rmd160", &buffer, &size));
  EXPECT_EQ(content, string(reinterpret_cast<char *>(buffer), size));
  free(buffer);
  EXPECT_TRUE(cache_mgr_->Open2Mem(id_shake128, "id_shake128", &buffer, &size));
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
  EXPECT_TRUE(cache_mgr_->CommitFromMem(id_even, buffer, size_even, "even"));
  EXPECT_TRUE(cache_mgr_->CommitFromMem(id_odd, buffer, size_odd, "odd"));

  unsigned char *read_buffer;
  uint64_t size;
  EXPECT_TRUE(cache_mgr_->Open2Mem(id_even, "even", &read_buffer, &size));
  EXPECT_EQ(size, size_even);
  EXPECT_EQ(0, memcmp(read_buffer, buffer, size_even));
  free(read_buffer);
  EXPECT_TRUE(cache_mgr_->Open2Mem(id_odd, "odd", &read_buffer, &size));
  EXPECT_EQ(size, size_odd);
  EXPECT_EQ(0, memcmp(read_buffer, buffer, size_odd));
  free(read_buffer);

  int fd = cache_mgr_->Open(CacheManager::Bless(id_odd));
  EXPECT_GE(fd, 0);
  EXPECT_EQ(0, cache_mgr_->Pread(fd, NULL, 0, 0));

  read_buffer = reinterpret_cast<unsigned char *> (
    smalloc(2 * cache_mgr_->max_object_size()));
  EXPECT_EQ(0, cache_mgr_->Pread(fd, read_buffer, 0, size_odd));
  EXPECT_EQ(-EINVAL, cache_mgr_->Pread(fd, read_buffer, 1, size_odd));
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
      << next_size << " bytes requested, " << bytes_read << " bytes received, "
      << "read so far: " << total_size << "/" << size_odd << " bytes";
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
  EXPECT_TRUE(cache_mgr_->Open2Mem(id, "test", &buf, &size));
  EXPECT_EQ(content, string(reinterpret_cast<char *>(buf), size));
  free(buf);
}
