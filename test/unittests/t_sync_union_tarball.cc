/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>
#include <unistd.h>

#include <cassert>
#include <string>

#include "aux/tar_files.h"
#include "mock/m_sync_mediator.h"
#include "sync_item.h"
#include "sync_union_tarball.h"
#include <memory>
#include "util/posix.h"
#include "util/string.h"

namespace {

class T_SyncUnionTarball : public ::testing::Test {
 protected:
  void SetUp() { m_sync_mediator_ = new publish::MockSyncMediator(); }

  std::string CreateTarFile(const std::string &tar_filename,
                            const std::string &base64_data) {
    std::string data_binary;
    Debase64(base64_data, &data_binary);

    std::string tmp_dir = CreateTempDir("test_sync_union");
    assert(!tmp_dir.empty());
    tmp_tar_filename_ = CreateTempPath(tmp_dir + "t_sync_union_tarball", 0666);
    assert(!tmp_tar_filename_.empty());
    assert(SafeWriteToFile(data_binary, tmp_tar_filename_, 0600));

    return tmp_tar_filename_;
  }

  virtual void TearDown() { unlink(tmp_tar_filename_.c_str()); }

  std::unique_ptr<publish::MockSyncMediator> m_sync_mediator_;
  std::string tmp_tar_filename_;
};

TEST_F(T_SyncUnionTarball, Simple) {
  std::string tar_filename = CreateTarFile("tar.tar", simple_tar);
  publish::SyncUnionTarball sync_union(m_sync_mediator_.get(), "",
                                       tar_filename, "tmpsync", -1u, -1u, "",
                                       false);

  EXPECT_TRUE(sync_union.Initialize());
  EXPECT_EQ(1, m_sync_mediator_->n_register);

  sync_union.Traverse();
  EXPECT_EQ(2, m_sync_mediator_->n_reg);
  EXPECT_EQ(3, m_sync_mediator_->n_dir);
}

TEST_F(T_SyncUnionTarball, FourEmptyFiles) {
  std::string tar_filename = CreateTarFile("tar.tar", four_empty_files);
  publish::SyncUnionTarball sync_union(m_sync_mediator_.get(), "",
                                       tar_filename, "tmpsync", -1u, -1u, "",
                                       false);

  EXPECT_TRUE(sync_union.Initialize());
  EXPECT_EQ(1, m_sync_mediator_->n_register);

  sync_union.Traverse();
  EXPECT_EQ(4, m_sync_mediator_->n_reg);
  EXPECT_EQ(1, m_sync_mediator_->n_dir);
}

TEST_F(T_SyncUnionTarball, Complex) {
  std::string tar_filename = CreateTarFile("tar.tar", complex_tar);
  publish::SyncUnionTarball sync_union(m_sync_mediator_.get(), "",
                                       tar_filename, "tmpsync", -1u, -1u, "",
                                       false);

  EXPECT_TRUE(sync_union.Initialize());
  EXPECT_EQ(1, m_sync_mediator_->n_register);

  sync_union.Traverse();
  EXPECT_EQ(3, m_sync_mediator_->n_reg);
  EXPECT_EQ(1, m_sync_mediator_->n_lnk);
  EXPECT_EQ(3, m_sync_mediator_->n_dir);
}

TEST_F(T_SyncUnionTarball, FastDeletePassedToRemove) {
  // Create a SyncUnionTarball with fast_delete=true and a to_delete path
  // but no actual tarball (empty string means delete-only mode)
  publish::SyncUnionTarball sync_union(m_sync_mediator_.get(), "",
                                       "", "", -1u, -1u,
                                       "some/nested/dir",
                                       false /* create_catalog_on_root */,
                                       true  /* fast_delete */);

  EXPECT_TRUE(sync_union.Initialize());

  sync_union.Traverse();
  // Verify Remove was called with fast_delete=true
  EXPECT_EQ(1, m_sync_mediator_->n_remove);
  EXPECT_TRUE(m_sync_mediator_->last_fast_delete);
}

TEST_F(T_SyncUnionTarball, NoFastDeleteByDefault) {
  // Create a SyncUnionTarball with default fast_delete (false) and a
  // to_delete path
  publish::SyncUnionTarball sync_union(m_sync_mediator_.get(), "",
                                       "", "", -1u, -1u,
                                       "some/dir",
                                       false /* create_catalog_on_root */);

  EXPECT_TRUE(sync_union.Initialize());

  sync_union.Traverse();
  // Verify Remove was called with fast_delete=false (default)
  EXPECT_EQ(1, m_sync_mediator_->n_remove);
  EXPECT_FALSE(m_sync_mediator_->last_fast_delete);
}

TEST_F(T_SyncUnionTarball, FastDeleteMultiplePaths) {
  // Test that fast_delete is passed for each path when deleting multiple
  // paths (using the path delimiter ":")
  publish::SyncUnionTarball sync_union(m_sync_mediator_.get(), "",
                                       "", "", -1u, -1u,
                                       "dir/a:dir/b",
                                       false /* create_catalog_on_root */,
                                       true  /* fast_delete */);

  EXPECT_TRUE(sync_union.Initialize());

  sync_union.Traverse();
  // Verify Remove was called twice, both with fast_delete=true
  EXPECT_EQ(2, m_sync_mediator_->n_remove);
  EXPECT_TRUE(m_sync_mediator_->last_fast_delete);
}

}  // namespace
