/**
 * This file is part of the CernVM File System.
 */

#include <sync_union_tarball.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cassert>
#include <iostream>
#include <string>

#include "mock/m_sync_mediator.h"

#include "util/string.h"

#include "sync_item.h"

#include "aux/tar_files.h"

using ::testing::_;
using ::testing::DefaultValue;
using ::testing::Return;
using ::testing::Property;

using ::publish::SyncItem;

namespace {

class T_SyncUnionTarball : public ::testing::Test {
 protected:
  void SetUp() {
    zlib::Algorithms algo;
    DefaultValue<zlib::Algorithms>::Set(algo);

    m_sync_mediator_ = new publish::MockSyncMediator();
  }

  std::string CreateTarFile(const std::string& tar_filename,
                            const std::string& base64_data) {
    int tar;
    std::string data_binary;
    Debase64(base64_data, &data_binary);

    std::string tmp_dir = CreateTempDir("test_sync_union");
    assert(!tmp_dir.empty());
    tmp_tar_filename_ = CreateTempPath(tmp_dir + "t_sync_union_tarball", 0666);
    assert(!tmp_tar_filename_.empty());

    tar = open(tmp_tar_filename_.c_str(), O_WRONLY);

    assert(tar > 0);
    assert(SafeWrite(tar, data_binary.c_str(), data_binary.size()));

    return tmp_tar_filename_;
  }

  virtual void TearDown() {
    remove(tmp_tar_filename_.c_str());
    delete m_sync_mediator_;
    DefaultValue<zlib::Algorithms>::Clear();
  }

  publish::MockSyncMediator* m_sync_mediator_;
  std::string tmp_tar_filename_;
};

TEST_F(T_SyncUnionTarball, Init) {
  std::string tar_filename = CreateTarFile("tar.tar", simple_tar);
  publish::SyncUnionTarball sync_union(m_sync_mediator_, "", "", "",
                                       tar_filename, "/tmp/lala");

  EXPECT_CALL(*m_sync_mediator_, RegisterUnionEngine(_)).Times(1);
  EXPECT_TRUE(sync_union.Initialize());
}

TEST_F(T_SyncUnionTarball, Traverse) {
  std::string tar_filename = CreateTarFile("tar.tar", simple_tar);
  publish::SyncUnionTarball sync_union(m_sync_mediator_, "", "", "",
                                       tar_filename, "/tmp/lala");

  EXPECT_CALL(*m_sync_mediator_, RegisterUnionEngine(_)).Times(1);
  sync_union.Initialize();

  // We enter the directory, check that it is a new directory, since it is a new
  // directory we don't propagate down the recursion.
  // The down propagation is done by SyncMediator::AddDirectoryRecursively which
  // uses privates methods.
  // Then we add the same directory and finally we leave the directory.
  EXPECT_CALL(*m_sync_mediator_, EnterDirectory(_)).Times(1);
  EXPECT_CALL(*m_sync_mediator_,
              Add(AllOf(Property(&SyncItem::IsDirectory, true),
                        Property(&SyncItem::filename, "tar"))))
      .Times(1);
  EXPECT_CALL(*m_sync_mediator_, LeaveDirectory(_)).Times(1);
  sync_union.Traverse();
}

TEST_F(T_SyncUnionTarball, Four_Empty_Files) {
  std::string tar_filename = CreateTarFile("4_empty.tar", four_empty_files);
  publish::SyncUnionTarball sync_union(m_sync_mediator_, "", "", "",
                                       tar_filename, "/tmp/lala");

  EXPECT_CALL(*m_sync_mediator_, RegisterUnionEngine(_)).Times(1);
  sync_union.Initialize();

  EXPECT_CALL(*m_sync_mediator_, EnterDirectory(_)).Times(1);
  EXPECT_CALL(*m_sync_mediator_, IsExternalData())
      .Times(4)
      .WillRepeatedly(Return(false));
  EXPECT_CALL(*m_sync_mediator_, GetCompressionAlgorithm()).Times(4);

  EXPECT_CALL(*m_sync_mediator_,
              Add(AllOf(Property(&SyncItem::IsRegularFile, true),
                        Property(&SyncItem::filename, "bar"))))
      .Times(1);
  EXPECT_CALL(*m_sync_mediator_,
              Add(AllOf(Property(&SyncItem::IsRegularFile, true),
                        Property(&SyncItem::filename, "baz"))))
      .Times(1);
  EXPECT_CALL(*m_sync_mediator_,
              Add(AllOf(Property(&SyncItem::IsRegularFile, true),
                        Property(&SyncItem::filename, "foo"))))
      .Times(1);
  EXPECT_CALL(*m_sync_mediator_,
              Add(AllOf(Property(&SyncItem::IsRegularFile, true),
                        Property(&SyncItem::filename, "fuz"))))
      .Times(1);

  EXPECT_CALL(*m_sync_mediator_, LeaveDirectory(_)).Times(1);
  sync_union.Traverse();
}

TEST_F(T_SyncUnionTarball, Complex_Tar) {
  std::string tar_filename = CreateTarFile("complex_tar.tar", complex_tar);
  publish::SyncUnionTarball sync_union(m_sync_mediator_, "", "", "",
                                       tar_filename, "/tmp/lala");

  EXPECT_CALL(*m_sync_mediator_, RegisterUnionEngine(_)).Times(1);
  sync_union.Initialize();

  EXPECT_CALL(*m_sync_mediator_, EnterDirectory(_)).Times(1);
  // Once for each "Regular file", the one with entry_type == kItemFile
  EXPECT_CALL(*m_sync_mediator_, IsExternalData())
      .Times(2)
      .WillRepeatedly(Return(false));

  // Similarly as above, one call for each "Regular file", the one with
  // entry_type == kItemFile
  EXPECT_CALL(*m_sync_mediator_, GetCompressionAlgorithm()).Times(2);

  EXPECT_CALL(*m_sync_mediator_,
              Add(AllOf(Property(&SyncItem::IsRegularFile, true),
                        Property(&SyncItem::filename, "bar"))))
      .Times(1);
  EXPECT_CALL(*m_sync_mediator_,
              Add(AllOf(Property(&SyncItem::IsDirectory, true),
                        Property(&SyncItem::filename, "dir"))))
      .Times(1);
  EXPECT_CALL(*m_sync_mediator_,
              Add(AllOf(Property(&SyncItem::IsRegularFile, true),
                        Property(&SyncItem::filename, "foo"))))
      .Times(1);
  EXPECT_CALL(*m_sync_mediator_,
              Add(AllOf(Property(&SyncItem::IsSymlink, true),
                        Property(&SyncItem::filename, "foo_link"))))
      .Times(1);

  EXPECT_CALL(*m_sync_mediator_, LeaveDirectory(_)).Times(1);
  sync_union.Traverse();
}

}  // namespace
