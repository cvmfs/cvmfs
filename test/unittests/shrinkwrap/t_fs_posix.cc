/**
 * This file is part of the CernVM File System.
 */
#include <gtest/gtest.h>

#include <errno.h>
#include <unistd.h>

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
  MkdirDeep("./TestStat/abc/", 0744, true);
  CreateFile("./TestStat/abc/testfile1.txt", 0744, false);
  struct cvmfs_attr *stat = cvmfs_attr_init();
  ASSERT_EQ(0,
    interface_->get_stat(
      interface_->context_,
      "/abc/testfile1.txt",
      stat, true));
  // Check empty checksum
  ASSERT_STREQ("da39a3ee5e6b4b0d3255bfef95601890afd80709", stat->cvm_checksum);
  ASSERT_EQ(0744, (~S_IFMT) & stat->st_mode);
  ASSERT_EQ(0, stat->st_size);
  ASSERT_EQ(getuid(), stat->st_uid);
  ASSERT_EQ(getgid(), stat->st_gid);
  ASSERT_STREQ("testfile1.txt", stat->cvm_name);
  ASSERT_STREQ("/abc", stat->cvm_parent);
  cvmfs_attr_free(stat);
  stat = cvmfs_attr_init();
  ASSERT_EQ(-1,
    interface_->get_stat(
      interface_->context_,
      "/abc/idontexist.txt",
      stat, true));
  ASSERT_EQ(ENOENT, errno);
  cvmfs_attr_free(stat);
}

TEST_F(T_FS_Traversal_POSIX, TestSetMeta) {
  MkdirDeep("./TestSetMeta/abc/", 0744, true);
  struct cvmfs_attr *stat = cvmfs_attr_init();
  ASSERT_EQ(0,
    interface_->get_stat(
      interface_->context_,
      "/abc",
      stat, true));
  // Change a few easily modifiable values...
  stat->st_mode = 0700;
  XattrList *newXattrs = new XattrList();
  newXattrs->Set("user.test", "foo");
  if (stat->cvm_xattrs != NULL) {
    delete reinterpret_cast<XattrList *>(stat->cvm_xattrs);
  }
  stat->cvm_xattrs = newXattrs;
  ASSERT_EQ(0,
    interface_->set_meta(interface_->context_, "/abc", stat));
  // Just to make sure stat call actually changes the value
  stat->st_mode = 0;
  cvmfs_attr_free(stat);
  stat = cvmfs_attr_init();
  ASSERT_EQ(0,
    interface_->get_stat(
      interface_->context_,
      "/abc",
      stat, true));

  ASSERT_EQ(0700, (~S_IFMT) & stat->st_mode);
  XattrList *res_xattrs = reinterpret_cast<XattrList *>(stat->cvm_xattrs);
  std::string xattr_value_result;
  ASSERT_EQ(true, res_xattrs->Get("user.test", &xattr_value_result));
  ASSERT_STREQ("foo", xattr_value_result.c_str());
  cvmfs_attr_free(stat);
}

TEST_F(T_FS_Traversal_POSIX, TestGetIdentifier) {
  struct cvmfs_attr *stat = cvmfs_attr_init();
  stat->cvm_checksum = strdup("da39a3ee5e6b4b0d3255bfef95601890afd80709");
  char * res = interface_->get_identifier(interface_->context_, stat);
  shash::Any result = HashMeta(stat);
  std::string result_path
    = std::string("/da/39/a3ee5e6b4b0d3255bfef95601890afd80709.")
      + result.ToString();
  ASSERT_STREQ(result_path.c_str(), res);
  free(res);
  cvmfs_attr_free(stat);
}

TEST_F(T_FS_Traversal_POSIX, TestTouchLinkUnlink) {
  MkdirDeep("./TestTouchLinkUnlink/def/", 0744, true);
  struct cvmfs_attr *stat = cvmfs_attr_init();
  // Other checksum this time to avoid collisions during touch
  stat->cvm_checksum = strdup("da39a3ee5e6b4b0d3255bfef95601890afd8070a");
  // Avoid need of sudo rights by using current user for file creation
  stat->st_gid = getgid();
  stat->st_uid = getuid();
  stat->st_mode = 0700;
  char *ident1 = interface_->get_identifier(interface_->context_, stat);
  // Some error cases
  ASSERT_EQ(-1, interface_->do_link(interface_->context_,
  "/foo.txt", ident1));
  ASSERT_EQ(ENOENT, errno);
  ASSERT_EQ(-1, interface_->do_link(interface_->context_,
  "/abc/foo.txt", ident1));
  ASSERT_EQ(ENOENT, errno);
  ASSERT_EQ(-1, interface_->do_unlink(interface_->context_,
  "/foo.txt"));
  ASSERT_EQ(ENOENT, errno);
  ASSERT_EQ(-1, interface_->do_unlink(interface_->context_,
  "/def"));
  ASSERT_EQ(EISDIR, errno);

  // Check if link produces actual hardlink (same inode)
  ASSERT_EQ(0, interface_->touch(interface_->context_, stat));
  // Should succeed exactly once
  ASSERT_EQ(-1, interface_->touch(interface_->context_, stat));
  ASSERT_EQ(0, interface_->do_link(interface_->context_,
  "/foo.txt", ident1));
  struct stat cache_link_stat;
  struct stat link_stat;
  std::string cache_link_path1 = std::string("./.data/") + ident1;
  ASSERT_EQ(0, lstat(cache_link_path1.c_str(), &cache_link_stat));
  ASSERT_EQ(0, lstat("./TestTouchLinkUnlink/foo.txt", &link_stat));
  ASSERT_EQ(cache_link_stat.st_ino, link_stat.st_ino);

  // Try to rewrite link...
  stat->st_mode = 0770;
  char *ident2 = interface_->get_identifier(interface_->context_, stat);
  ASSERT_EQ(0, interface_->touch(interface_->context_, stat));
  ASSERT_EQ(0, interface_->do_link(interface_->context_,
  "/foo.txt", ident2));
  std::string cache_link_path2 = std::string("./.data/") + ident2;
  ASSERT_EQ(0, lstat(cache_link_path2.c_str(), &cache_link_stat));
  ASSERT_EQ(0, lstat("./TestTouchLinkUnlink/foo.txt", &link_stat));
  ASSERT_EQ(cache_link_stat.st_ino, link_stat.st_ino);

  // Check unlink
  ASSERT_EQ(0, interface_->do_unlink(interface_->context_,
  "/foo.txt"));
  ASSERT_EQ(-1, lstat("./TestTouchLinkUnlink/foo.txt", &link_stat));

  free(ident1);
  free(ident2);
  cvmfs_attr_free(stat);
}

TEST_F(T_FS_Traversal_POSIX, TestSymlink) {
  struct cvmfs_attr *stat = cvmfs_attr_init();
  interface_->do_symlink(interface_->context_,
    "/asymlink",
    "somedestination",
    stat);
  struct stat buf;
  ASSERT_EQ(0, lstat("./TestSymlink/asymlink", &buf));
  ASSERT_TRUE(S_ISLNK(buf.st_mode));
  char strbuf[17];
  size_t read_len
    = readlink("./TestSymlink/asymlink", strbuf, sizeof(strbuf)-1);
  strbuf[read_len] = '\0';
  ASSERT_STREQ("somedestination", strbuf);
  cvmfs_attr_free(stat);
}

TEST_F(T_FS_Traversal_POSIX, TestReadWrite) {
  // TODO(steuber): Write tests here
}

TEST_F(T_FS_Traversal_POSIX, TestMkDirRmDir) {
  MkdirDeep("./TestMkDirRmDir/abc/", 0744, true);
  MkdirDeep("./TestMkDirRmDir/abc/def", 0744, true);
  struct cvmfs_attr *stat = cvmfs_attr_init();
  struct stat buf;
  stat->st_gid = getgid();
  stat->st_uid = getuid();
  stat->st_mode = 0700;

  // Check a few cases which should fail
  ASSERT_EQ(-1, interface_->do_mkdir(interface_->context_, "/def/ghi", stat));
  ASSERT_EQ(ENOENT, errno);
  ASSERT_EQ(-1, interface_->do_rmdir(interface_->context_, "/def"));
  ASSERT_EQ(ENOENT, errno);
  ASSERT_EQ(-1, interface_->do_rmdir(interface_->context_, "/abc"));
  ASSERT_EQ(ENOTEMPTY, errno);

  // Try to rewrite a directory
  ASSERT_EQ(0, interface_->do_mkdir(interface_->context_, "/abc", stat));
  ASSERT_EQ(0, lstat("./TestMkDirRmDir/abc", &buf));
  ASSERT_EQ(0700, ((~S_IFMT) & stat->st_mode));
  ASSERT_EQ(-1, lstat("./TestMkDirRmDir/abc/def", &buf));

  // Try to create simple new directory
  ASSERT_EQ(0, interface_->do_mkdir(interface_->context_, "/abc/def", stat));
  ASSERT_EQ(0, lstat("./TestMkDirRmDir/abc", &buf));
  // Delete them again
  ASSERT_EQ(0, interface_->do_rmdir(interface_->context_, "/abc/def"));
  ASSERT_EQ(-1, lstat("./TestMkDirRmDir/abc/def", &buf));
  ASSERT_EQ(0, interface_->do_rmdir(interface_->context_, "/abc"));
  ASSERT_EQ(-1, lstat("./TestMkDirRmDir/abc", &buf));

  cvmfs_attr_free(stat);
}

TEST_F(T_FS_Traversal_POSIX, TestHasFile) {
  CreateFile("./.data/ff/ff/test.txt", 0744, false);
  ASSERT_TRUE(interface_->has_file(
    interface_->context_, "/ff/ff/test.txt"));
}

TEST(T_GarbageCollection_POSIX, TestGarbageCollection) {
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
