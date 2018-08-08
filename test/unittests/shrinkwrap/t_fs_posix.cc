/**
 * This file is part of the CernVM File System.
 */
#include <gtest/gtest.h>

#include "libcvmfs.h"
#include "util/posix.h"

#include "shrinkwrap/fs_traversal_interface.h"
#include "shrinkwrap/posix/interface.h"
#include "shrinkwrap/util.h"
#include "test-util.h"

class T_FS_Traversal_POSIX : public ::testing::Test {
 protected:
  virtual void SetUp() {
    interface_ = posix_get_interface();
    const char *test_name
      = ::testing::UnitTest::GetInstance()->current_test_info()->name();
    interface_->context_ = interface_->initialize(test_name,
      "./", NULL, NULL, 0);
  }
  virtual void TearDown() {
    interface_->finalize(interface_->context_);
    interface_->context_ = NULL;
    delete interface_;
    interface_ = NULL;
  }
  struct fs_traversal *interface_;
};

TEST_F(T_FS_Traversal_POSIX, TestInit) {
  ASSERT_TRUE(DirectoryExists("./TestInit"));
  ASSERT_TRUE(DirectoryExists("./.data"));
  ASSERT_TRUE(DirectoryExists("./.data/ff/ff"));
  interface_->finalize(interface_->context_);
  interface_->context_ = interface_->initialize("TestInit-2",
      "./", "./.data-TestInit", NULL, 0);
  ASSERT_TRUE(DirectoryExists("./TestInit-2"));
  ASSERT_TRUE(DirectoryExists("./.data-TestInit"));
  ASSERT_TRUE(DirectoryExists("./.data-TestInit/ff/ff"));
}

TEST_F(T_FS_Traversal_POSIX, TestListDir) {
  ASSERT_EQ(0, mkdir("./TestListDir/testdir", 0700));
  CreateFile("./TestListDir/testfile1.txt", 0700, false);
  CreateFile("./TestListDir/testdir/testfile2.txt", 0700, false);
  CreateFile("./TestListDir/testdir/testfile3.txt", 0700, false);
  size_t len = 0;
  char **list;
  interface_->list_dir(interface_->context_, "", &list, &len);
  ASSERT_EQ(2, len);
  AssertListHas("testfile1.txt", list, len);
  AssertListHas("testdir", list, len);
  FreeList(list, len);
  len = 0;
  interface_->list_dir(interface_->context_, "testdir", &list, &len);
  ASSERT_EQ(2, len);
  AssertListHas("testfile2.txt", list, len);
  AssertListHas("testfile3.txt", list, len);
  FreeList(list, len);
  len = 1;
  interface_->list_dir(interface_->context_, "idontexist", &list, &len);
  ASSERT_EQ(0, len);
  ASSERT_EQ(NULL, *list);
  FreeList(list, len);
}

TEST_F(T_FS_Traversal_POSIX, TestStat) {
  CreateFile("./TestStat/testfile1.txt", 0744, false);
  struct cvmfs_attr stat;
  ASSERT_EQ(0,
    interface_->get_stat(
      interface_->context_,
      "testfile1.txt",
      &stat, true));
  // Check empty checksum
  ASSERT_STREQ("da39a3ee5e6b4b0d3255bfef95601890afd80709", stat.cvm_checksum);
  ASSERT_EQ(0744, (~S_IFMT) & stat.st_mode);
}

TEST(T_Fs_Traversal_POSIX, TestGarbageCollection) {
  struct fs_traversal *dest = posix_get_interface();
  struct fs_traversal_context *context
    = dest->initialize("./", "posix", "./data", NULL, 4);

  std::string content1 = "a";
  shash::Any content1_hash(shash::kSha1);
  shash::HashString(content1, &content1_hash);
  std::string content2 = "b";
  shash::Any content2_hash(shash::kSha1);
  shash::HashString(content2, &content2_hash);
  XattrList *xlist1 = create_sample_xattrlist("TestGarbageCollection1");
  XattrList *xlist2 = create_sample_xattrlist("TestGarbageCollection2");
  struct cvmfs_attr *stat1 = create_sample_stat("foo", 0, 0777, 0, xlist1,
    &content1_hash);
  struct cvmfs_attr *stat2 = create_sample_stat("foo", 0, 0777, 0, xlist1,
    &content2_hash);
  struct cvmfs_attr *stat3 = create_sample_stat("foo", 0, 0777, 0, xlist2,
    &content1_hash);
  struct cvmfs_attr *stat4 = create_sample_stat("foo", 0, 0777, 0, xlist2,
    &content2_hash);

  dest->touch(context, stat1);
  const char *ident1 = dest->get_identifier(context, stat1);
  dest->do_link(context, "file1.txt", ident1);
  dest->touch(context, stat2);
  const char *ident2 = dest->get_identifier(context, stat2);
  dest->do_link(context, "file1.txt", ident2);  // unlinks ident1
  dest->touch(context, stat3);
  const char *ident3 = dest->get_identifier(context, stat3);
  dest->do_link(context, "file3.txt", ident3);
  dest->touch(context, stat4);
  const char *ident4 = dest->get_identifier(context, stat4);
  dest->do_link(context, "file4.txt", ident4);

  dest->do_unlink(context, "file3.txt");

  dest->finalize(context);
  context = dest->initialize("./", "posix", "./data", NULL, 4);
  dest->garbage_collector(context);

  std::string data_base_path = "./data/";
  ASSERT_STRNE(ident1, ident2);
  ASSERT_STRNE(ident1, ident3);
  ASSERT_STRNE(ident1, ident4);
  ASSERT_STRNE(ident2, ident3);
  ASSERT_STRNE(ident2, ident4);
  ASSERT_STRNE(ident3, ident4);
  ASSERT_FALSE(FileExists(data_base_path + ident1));
  ASSERT_TRUE(FileExists(data_base_path + ident2));
  ASSERT_FALSE(FileExists(data_base_path + ident3));
  ASSERT_TRUE(FileExists(data_base_path + ident4));

  delete stat1;
  delete stat2;
  delete stat3;
  delete stat4;
  delete xlist1;
  delete xlist2;
}
