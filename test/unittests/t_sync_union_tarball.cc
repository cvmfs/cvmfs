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
#include "util/pointer.h"
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

  UniquePtr<publish::MockSyncMediator> m_sync_mediator_;
  std::string tmp_tar_filename_;
};

TEST_F(T_SyncUnionTarball, Simple) {
  std::string tar_filename = CreateTarFile("tar.tar", simple_tar);
  publish::SyncUnionTarball sync_union(m_sync_mediator_.weak_ref(), "",
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
  publish::SyncUnionTarball sync_union(m_sync_mediator_.weak_ref(), "",
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
  publish::SyncUnionTarball sync_union(m_sync_mediator_.weak_ref(), "",
                                       tar_filename, "tmpsync", -1u, -1u, "",
                                       false);

  EXPECT_TRUE(sync_union.Initialize());
  EXPECT_EQ(1, m_sync_mediator_->n_register);

  sync_union.Traverse();
  EXPECT_EQ(3, m_sync_mediator_->n_reg);
  EXPECT_EQ(1, m_sync_mediator_->n_lnk);
  EXPECT_EQ(3, m_sync_mediator_->n_dir);
}

}  // namespace
