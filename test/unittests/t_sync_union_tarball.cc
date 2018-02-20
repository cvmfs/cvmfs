/**
 * This file is part of the CernVM File System.
 */

#include <sync_union_tarball.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <util/string.h>

#include <fstream>
#include <iostream>

#include "mock/m_sync_mediator.h"

#include "sync_item.h"

#include "tar_files.h"

using ::testing::_;
using ::testing::DefaultValue;
using ::testing::Return;
using ::testing::Property;

using ::publish::SyncItem;

namespace {

class T_Sync_union_tarball : public ::testing::Test {
 protected:
  void SetUp() {
    zlib::Algorithms algo;
    DefaultValue<zlib::Algorithms>::Set(algo);

    m_sync_mediator = new publish::MockSyncMediator();
  }

  std::string CreateTarFile(std::string tar_filename, std::string base64_data) {
    std::ofstream tar;
    std::string data_binary;
    Debase64(base64_data, &data_binary);

    tmp_tar_filename = "/tmp/" + tar_filename;

    tar.open(tmp_tar_filename.c_str(), std::ios::binary);

    if (!tar.is_open()) {
      cout << "Error: " << strerror(errno);
      assert(tar.is_open());
    }

    assert(tar.write(data_binary.c_str(), data_binary.size()));
    tar.close();

    return tmp_tar_filename;
  }

  virtual void TearDown() {
    remove(tmp_tar_filename.c_str());
    delete m_sync_mediator;
    DefaultValue<zlib::Algorithms>::Clear();
  }

  publish::MockSyncMediator* m_sync_mediator;
  std::string tmp_tar_filename;
};

TEST_F(T_Sync_union_tarball, Init) {
  std::string tar_filename = CreateTarFile("tar.tar", simple_tar);
  publish::SyncUnionTarball sync_union(m_sync_mediator, "", "", "",
                                       tar_filename, "/tmp/lala");

  EXPECT_CALL(*m_sync_mediator, RegisterUnionEngine(_)).Times(1);
  EXPECT_TRUE(sync_union.Initialize());
}

TEST_F(T_Sync_union_tarball, Traverse) {
  std::string tar_filename = CreateTarFile("tar.tar", simple_tar);
  publish::SyncUnionTarball sync_union(m_sync_mediator, "", "", "",
                                       tar_filename, "/tmp/lala");

  EXPECT_CALL(*m_sync_mediator, RegisterUnionEngine(_)).Times(1);
  sync_union.Initialize();

  // We enter the directory, check that it is a new directory, since it is a new
  // directory we don't propagate down the recursion.
  // The down propagation is done by SyncMediator::AddDirectoryRecursively which
  // uses privates methods.
  // Then we add the same directory and finally we leave the directory.
  EXPECT_CALL(*m_sync_mediator, EnterDirectory(_)).Times(1);
  EXPECT_CALL(*m_sync_mediator,
              Add(AllOf(Property(&SyncItem::IsDirectory, true),
                        Property(&SyncItem::filename, "tar"))))
      .Times(1);
  EXPECT_CALL(*m_sync_mediator, LeaveDirectory(_)).Times(1);
  sync_union.Traverse();
}

TEST_F(T_Sync_union_tarball, Four_Empty_Files) {
  std::string tar_filename = CreateTarFile("4_empty.tar", four_empty_files);
  publish::SyncUnionTarball sync_union(m_sync_mediator, "", "", "",
                                       tar_filename, "/tmp/lala");

  EXPECT_CALL(*m_sync_mediator, RegisterUnionEngine(_)).Times(1);
  sync_union.Initialize();

  EXPECT_CALL(*m_sync_mediator, EnterDirectory(_)).Times(1);
  EXPECT_CALL(*m_sync_mediator, IsExternalData())
      .Times(4)
      .WillRepeatedly(Return(false));
  EXPECT_CALL(*m_sync_mediator, GetCompressionAlgorithm()).Times(4);

  EXPECT_CALL(*m_sync_mediator,
              Add(AllOf(Property(&SyncItem::IsRegularFile, true),
                        Property(&SyncItem::filename, "bar"))))
      .Times(1);
  EXPECT_CALL(*m_sync_mediator,
              Add(AllOf(Property(&SyncItem::IsRegularFile, true),
                        Property(&SyncItem::filename, "baz"))))
      .Times(1);
  EXPECT_CALL(*m_sync_mediator,
              Add(AllOf(Property(&SyncItem::IsRegularFile, true),
                        Property(&SyncItem::filename, "foo"))))
      .Times(1);
  EXPECT_CALL(*m_sync_mediator,
              Add(AllOf(Property(&SyncItem::IsRegularFile, true),
                        Property(&SyncItem::filename, "fuz"))))
      .Times(1);

  EXPECT_CALL(*m_sync_mediator, LeaveDirectory(_)).Times(1);
  sync_union.Traverse();
}

TEST_F(T_Sync_union_tarball, Complex_Tar) {
  std::string tar_filename = CreateTarFile("complex_tar.tar", complex_tar);
  publish::SyncUnionTarball sync_union(m_sync_mediator, "", "", "",
                                       tar_filename, "/tmp/lala");

  EXPECT_CALL(*m_sync_mediator, RegisterUnionEngine(_)).Times(1);
  sync_union.Initialize();

  EXPECT_CALL(*m_sync_mediator, EnterDirectory(_)).Times(1);
  // Once for each "Regular file", the one with entry_type == kItemFile
  EXPECT_CALL(*m_sync_mediator, IsExternalData())
      .Times(2)
      .WillRepeatedly(Return(false));

  // Similarly as above, one call for each "Regular file", the one with
  // entry_type == kItemFile
  EXPECT_CALL(*m_sync_mediator, GetCompressionAlgorithm()).Times(2);

  EXPECT_CALL(*m_sync_mediator,
              Add(AllOf(Property(&SyncItem::IsRegularFile, true),
                        Property(&SyncItem::filename, "bar"))))
      .Times(1);
  EXPECT_CALL(*m_sync_mediator,
              Add(AllOf(Property(&SyncItem::IsDirectory, true),
                        Property(&SyncItem::filename, "dir"))))
      .Times(1);
  EXPECT_CALL(*m_sync_mediator,
              Add(AllOf(Property(&SyncItem::IsRegularFile, true),
                        Property(&SyncItem::filename, "foo"))))
      .Times(1);
  EXPECT_CALL(*m_sync_mediator,
              Add(AllOf(Property(&SyncItem::IsSymlink, true),
                        Property(&SyncItem::filename, "foo_link"))))
      .Times(1);

  EXPECT_CALL(*m_sync_mediator, LeaveDirectory(_)).Times(1);
  sync_union.Traverse();
}

}  // namespace
