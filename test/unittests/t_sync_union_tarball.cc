/**
 * This file is part of the CernVM File System.
 */

#include <sync_union_tarball.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cassert>
#include <string>

#include "aux/tar_files.h"
#include "mock/m_sync_mediator.h"
#include "sync_item.h"
#include "util/shared_ptr.h"
#include "util/string.h"

using ::testing::_;
using ::testing::DefaultValue;
using ::testing::Return;
using ::testing::Property;

using ::publish::SyncItem;

namespace {

class T_SyncUnionTarball : public ::testing::Test {
 protected:
  void SetUp() {
    zlib::Algorithms algo = zlib::kZlibDefault;
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
                                       tar_filename, "/tmp/lala", "");

  EXPECT_CALL(*m_sync_mediator_, RegisterUnionEngine(_)).Times(1);
  EXPECT_TRUE(sync_union.Initialize());
}
//
///*
// * tar
// * ├── [4.0K]  aaa
// * │   └── [   7]  joker
// * └── [   7]  hero
// *
// */
//
//TEST_F(T_SyncUnionTarball, Traverse) {
//  std::string tar_filename = CreateTarFile("tar.tar", simple_tar);
//  publish::SyncUnionTarball sync_union(m_sync_mediator_, "/rdonly", "/union",
//                                       "/scratch", tar_filename, "tmp/lala",
//                                       "");
//
//  EXPECT_CALL(*m_sync_mediator_, RegisterUnionEngine(_)).Times(1);
//  sync_union.Initialize();
//  EXPECT_CALL(*m_sync_mediator_, Add(_)).Times(5);
//
//  /*
//  EXPECT_CALL(*m_sync_mediator_,
//              Add(Pointee(AllOf(Property(&SyncItem::IsDirectory, true),
//                                Property(&SyncItem::filename, "tar")))))
//      .Times(1);
//  EXPECT_CALL(*m_sync_mediator_,
//              Add(Pointee(AllOf(Property(&SyncItem::IsDirectory, true),
//                                Property(&SyncItem::filename, "aaa")))))
//      .Times(1);
//
//  EXPECT_CALL(*m_sync_mediator_,
//              Add(Pointee(AllOf(Property(&SyncItem::IsRegularFile, true),
//                                Property(&SyncItem::filename, "joker")))))
//      .Times(1);
//  EXPECT_CALL(*m_sync_mediator_,
//              Add(Pointee(AllOf(Property(&SyncItem::IsRegularFile, true),
//                                Property(&SyncItem::filename, "hero")))))
//      .Times(1);
//*/
//  printf("PRE\n\n\n");
//  sync_union.Traverse();
//    printf("POST\n\n\n");
//}
//
//TEST_F(T_SyncUnionTarball, Four_Empty_Files) {
//  std::string tar_filename = CreateTarFile("4_empty.tar", four_empty_files);
//  publish::SyncUnionTarball sync_union(m_sync_mediator_, "", "", "",
//                                       tar_filename, "/tmp/lala", "");
//
//  EXPECT_CALL(*m_sync_mediator_, RegisterUnionEngine(_)).Times(1);
//  sync_union.Initialize();
//  EXPECT_CALL(*m_sync_mediator_,
//              Add(Pointee(AllOf(Property(&SyncItem::IsRegularFile, true),
//                                Property(&SyncItem::filename, "bar")))))
//      .Times(1);
//  EXPECT_CALL(*m_sync_mediator_,
//              Add(Pointee(AllOf(Property(&SyncItem::IsRegularFile, true),
//                                Property(&SyncItem::filename, "baz")))))
//      .Times(1);
//  EXPECT_CALL(*m_sync_mediator_,
//              Add(Pointee(AllOf(Property(&SyncItem::IsRegularFile, true),
//                                Property(&SyncItem::filename, "foo")))))
//      .Times(1);
//  EXPECT_CALL(*m_sync_mediator_,
//              Add(Pointee(AllOf(Property(&SyncItem::IsRegularFile, true),
//                                Property(&SyncItem::filename, "fuz")))))
//      .Times(1);
//
//  sync_union.Traverse();
//}
//
//TEST_F(T_SyncUnionTarball, Complex_Tar) {
//  std::string tar_filename = CreateTarFile("complex_tar.tar", complex_tar);
//  publish::SyncUnionTarball sync_union(m_sync_mediator_, "", "", "",
//                                       tar_filename, "/tmp/lala", "");
//
//  EXPECT_CALL(*m_sync_mediator_, RegisterUnionEngine(_)).Times(1);
//  sync_union.Initialize();
//
//  EXPECT_CALL(*m_sync_mediator_,
//              Add(Pointee(AllOf(
//                  Property(&SyncItem::IsRegularFile, true),
//                  Property(&SyncItem::GetRelativePath, "/tmp/lala/bar")))))
//      .Times(1);
//
//  EXPECT_CALL(*m_sync_mediator_,
//              Add(Pointee(AllOf(
//                  Property(&SyncItem::IsDirectory, true),
//                  Property(&SyncItem::GetRelativePath, "/tmp/lala/dir")))))
//      .Times(1);
//  EXPECT_CALL(*m_sync_mediator_,
//              Add(Pointee(AllOf(Property(&SyncItem::IsDirectory, true),
//                                Property(&SyncItem::GetRelativePath,
//                                         "/tmp/lala/dir/inside_dir")))))
//      .Times(1);
//  EXPECT_CALL(*m_sync_mediator_,
//              Add(Pointee(AllOf(Property(&SyncItem::IsRegularFile, true),
//                                Property(&SyncItem::GetRelativePath,
//                                         "/tmp/lala/dir/inside_dir/foo")))))
//      .Times(1);
//  EXPECT_CALL(*m_sync_mediator_,
//              Add(Pointee(AllOf(
//                  Property(&SyncItem::IsRegularFile, true),
//                  Property(&SyncItem::GetRelativePath, "/tmp/lala/foo")))))
//      .Times(1);
//  EXPECT_CALL(*m_sync_mediator_,
//              Add(Pointee(AllOf(
//                  Property(&SyncItem::IsSymlink, true),
//                  Property(&SyncItem::GetRelativePath, "/tmp/lala/foo_link")))))
//      .Times(1);
//  sync_union.Traverse();
//}
//
}  // namespace
