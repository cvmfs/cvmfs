/**
 * This file is part of the CernVM File System.
 */
#include <gtest/gtest.h>

#include <errno.h>
#include <time.h>
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
  ASSERT_EQ(2U, len);
  AssertListHas("testfile1.txt", list, len);
  AssertListHas("testdir", list, len);
  FreeList(list, len);
  len = 0;
  interface_->list_dir(interface_->context_, "testdir", &list, &len);
  ASSERT_EQ(2U, len);
  AssertListHas("testfile2.txt", list, len);
  AssertListHas("testfile3.txt", list, len);
  FreeList(list, len);
  len = 1;
  interface_->list_dir(interface_->context_, "idontexist", &list, &len);
  ASSERT_EQ(0U, len);
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
  ASSERT_EQ(mode_t(0744), (~S_IFMT) & stat->st_mode);
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

  ASSERT_EQ(mode_t(0700), (~S_IFMT) & stat->st_mode);
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
  ASSERT_EQ(0, interface_->do_symlink(interface_->context_,
    "/asymlink",
    "somedestination",
    stat));
  struct stat buf;
  ASSERT_EQ(0, lstat("./TestSymlink/asymlink", &buf));
  ASSERT_TRUE(S_ISLNK(buf.st_mode));
  char strbuf[17];
  size_t read_len
    = readlink("./TestSymlink/asymlink", strbuf, sizeof(strbuf)-1);
  strbuf[read_len] = '\0';
  ASSERT_STREQ("somedestination", strbuf);
  ASSERT_EQ(0, interface_->do_symlink(interface_->context_,
    "/asymlink",
    "anotherdestinat",
    stat));
  read_len
    = readlink("./TestSymlink/asymlink", strbuf, sizeof(strbuf)-1);
  strbuf[read_len] = '\0';
  ASSERT_STREQ("anotherdestinat", strbuf);
  cvmfs_attr_free(stat);
}

TEST_F(T_FS_Traversal_POSIX, TestReadWrite) {
  std::string ident = "/ff/ff/test.txt";
  std::string path = "./.data/"+ident;
  CreateFile(path, 0744);
  void *handle = interface_->get_handle(interface_->context_, ident.c_str());
  ASSERT_TRUE(NULL != handle);

  size_t read_len;
  char dummybuf[1];
  FILE *fd;
  char buf1[4];
  char buf2[7];
  char buf3[10];

  // Write with interface
  ASSERT_EQ(0, interface_->do_fopen(handle, fs_open_write));
  ASSERT_EQ(-1, interface_->do_fread(handle, dummybuf, 1, &read_len));
  ASSERT_EQ(0, interface_->do_fwrite(handle, "foo", 3));
  ASSERT_EQ(0, interface_->do_fclose(handle));

  // Read (check) and append with posix (content should be foo)
  fd = fopen(path.c_str(), "a+");
  ASSERT_EQ(3U, fread(buf1, sizeof(char), 3, fd));
  buf1[3] = '\0';
  ASSERT_STREQ("foo", buf1);
  ASSERT_EQ(3U, fwrite("bar", sizeof(char), 3, fd));
  fclose(fd);

  // Read with interface (content should be foobar)
  ASSERT_EQ(0, interface_->do_fopen(handle, fs_open_read));
  ASSERT_EQ(-1, interface_->do_fwrite(handle, "foo", 3));
  ASSERT_EQ(0, interface_->do_fread(handle, buf2, 6, &read_len));
  buf2[6] = '\0';
  ASSERT_STREQ("foobar", buf2);
  ASSERT_EQ(0, interface_->do_fclose(handle));

  // Append with interface
  ASSERT_EQ(0, interface_->do_fopen(handle, fs_open_append));
  ASSERT_EQ(-1, interface_->do_fread(handle, dummybuf, 1, &read_len));
  ASSERT_EQ(0, interface_->do_fwrite(handle, "bar", 3));
  ASSERT_EQ(0, interface_->do_fclose(handle));

  // Read (check) with posix (content should be foobarbar)
  fd = fopen(path.c_str(), "r");
  ASSERT_EQ(9U, fread(buf3, sizeof(char), 9, fd));
  buf3[9] = '\0';
  ASSERT_STREQ("foobarbar", buf3);
  fclose(fd);

  interface_->do_ffree(handle);
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
  ASSERT_EQ(mode_t(0700), ((~S_IFMT) & stat->st_mode));
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
  XattrList *xlist11 = create_sample_xattrlist("TestGarbageCollection1");
  XattrList *xlist22 = create_sample_xattrlist("TestGarbageCollection2");
  struct cvmfs_attr *stat1 = create_sample_stat("foo", 0, 0777, 0, xlist1,
    &content1_hash);
  struct cvmfs_attr *stat2 = create_sample_stat("foo", 0, 0777, 0,
    xlist11, &content2_hash);
  struct cvmfs_attr *stat3 = create_sample_stat("foo", 0, 0777, 0, xlist2,
    &content1_hash);
  struct cvmfs_attr *stat4 = create_sample_stat("foo", 0, 0777, 0,
    xlist22, &content2_hash);

  ASSERT_EQ(0, dest->touch(context, stat1));
  char *ident1 = dest->get_identifier(context, stat1);
  dest->do_link(context, "file1.txt", ident1);
  ASSERT_EQ(0, dest->touch(context, stat2));
  char *ident2 = dest->get_identifier(context, stat2);
  dest->do_link(context, "file1.txt", ident2);  // unlinks ident1
  dest->touch(context, stat3);
  char *ident3 = dest->get_identifier(context, stat3);
  dest->do_link(context, "file3.txt", ident3);
  dest->touch(context, stat4);
  char *ident4 = dest->get_identifier(context, stat4);
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
  dest->finalize(context);
  delete dest;

  cvmfs_attr_free(stat1);
  cvmfs_attr_free(stat2);
  cvmfs_attr_free(stat3);
  cvmfs_attr_free(stat4);
  free(ident1);
  free(ident2);
  free(ident3);
  free(ident4);
}
