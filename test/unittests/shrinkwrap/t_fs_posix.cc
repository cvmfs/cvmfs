/**
 * This file is part of the CernVM File System.
 */
#include <errno.h>
#include <gtest/gtest.h>
#include <time.h>
#include <unistd.h>

#include <cstdio>

#include "libcvmfs.h"
#include "shrinkwrap/fs_traversal_interface.h"
#include "shrinkwrap/posix/interface.h"
#include "shrinkwrap/util.h"
#include "testutil_shrinkwrap.h"
#include "util/platform.h"
#include "util/posix.h"


class T_FsPosix : public ::testing::Test {
 protected:
  virtual void SetUp() {
    // TODO(jblomer): put in namespace, code style
    interface_ = posix_get_interface();
    const char *test_name = ::testing::UnitTest::GetInstance()
                                ->current_test_info()
                                ->name();
    interface_->context_ = interface_->initialize(test_name, "./", NULL, NULL,
                                                  0);
  }

  virtual void TearDown() {
    interface_->finalize(interface_->context_);
    interface_->context_ = NULL;
    delete interface_;
    interface_ = NULL;
  }

  struct fs_traversal *interface_;
};


TEST_F(T_FsPosix, Init) {
  EXPECT_TRUE(DirectoryExists("./Init"));
  EXPECT_TRUE(DirectoryExists("./.data"));
  EXPECT_TRUE(DirectoryExists("./.data/ff/ff"));
  interface_->finalize(interface_->context_);
  interface_->context_ = interface_->initialize("TestInit-2", "./",
                                                "./.data-TestInit", NULL, 0);
  EXPECT_TRUE(DirectoryExists("./TestInit-2"));
  EXPECT_TRUE(DirectoryExists("./.data-TestInit"));
  EXPECT_TRUE(DirectoryExists("./.data-TestInit/ff/ff"));
}


TEST_F(T_FsPosix, ListDir) {
  ASSERT_TRUE(MkdirDeep("./ListDir/testdir", 0700));
  const bool ignore_failure = false;
  CreateFile("./ListDir/testfile1.txt", 0700, ignore_failure);
  CreateFile("./ListDir/testdir/testfile2.txt", 0700, ignore_failure);
  CreateFile("./ListDir/testdir/testfile3.txt", 0700, ignore_failure);

  size_t len = 0;
  char **list;
  interface_->list_dir(interface_->context_, "", &list, &len);
  EXPECT_EQ(2U, len);
  ExpectListHas("testfile1.txt", list);
  ExpectListHas("testdir", list);
  FreeList(list, len);

  len = 0;
  interface_->list_dir(interface_->context_, "testdir", &list, &len);
  EXPECT_EQ(2U, len);
  ExpectListHas("testfile2.txt", list);
  ExpectListHas("testfile3.txt", list);
  FreeList(list, len);

  len = 1;
  interface_->list_dir(interface_->context_, "idontexist", &list, &len);
  EXPECT_EQ(0U, len);
  EXPECT_EQ(NULL, *list);
  FreeList(list, len);
}


TEST_F(T_FsPosix, Stat) {
  ASSERT_TRUE(MkdirDeep("./Stat/abc/", 0744));
  const bool ignore_failure = false;
  CreateFile("./Stat/abc/testfile1.txt", 0744, ignore_failure);
  platform_stat64 stat_reference;
  ASSERT_EQ(0, platform_stat("./Stat/abc/testfile1.txt", &stat_reference));

  struct cvmfs_attr *stat_traversal = cvmfs_attr_init();
  EXPECT_EQ(-1,
            interface_->get_stat(interface_->context_, "/abc/testfile1.txt",
                                 stat_traversal, true));
  EXPECT_EQ(0,
            interface_->get_stat(interface_->context_, "/abc/testfile1.txt",
                                 stat_traversal, false));
  // Check empty checksum
  EXPECT_EQ(NULL, stat_traversal->cvm_checksum);
  EXPECT_EQ(stat_reference.st_mode, stat_traversal->st_mode);
  EXPECT_EQ(0, stat_traversal->st_size);
  EXPECT_EQ(stat_reference.st_uid, stat_traversal->st_uid);
  EXPECT_EQ(stat_reference.st_gid, stat_traversal->st_gid);
  EXPECT_STREQ("testfile1.txt", stat_traversal->cvm_name);
  EXPECT_STREQ("/abc", stat_traversal->cvm_parent);
  cvmfs_attr_free(stat_traversal);

  stat_traversal = cvmfs_attr_init();
  EXPECT_EQ(-1,
            interface_->get_stat(interface_->context_, "/abc/idontexist.txt",
                                 stat_traversal, true));
  EXPECT_EQ(ENOENT, errno);
  cvmfs_attr_free(stat_traversal);
}


TEST_F(T_FsPosix, SetMetaData) {
  EXPECT_TRUE(MkdirDeep("./SetMetaData/abc", 0700));
  const bool supports_xattrs = SupportsXattrs("./SetMetaData/abc");
  const bool ignore_failure = false;
  CreateFile("./SetMetaData/abc/foo", 0644, ignore_failure);
  struct cvmfs_attr *stat = cvmfs_attr_init();
  ASSERT_EQ(
      0, interface_->get_stat(interface_->context_, "/abc/foo", stat, false));

  // Change a few easily modifiable values...
  stat->st_mode = 0600;
  if (supports_xattrs) {
    XattrList *new_xattrs = new XattrList();
    new_xattrs->Set("user.test", "bar");
    if (stat->cvm_xattrs != NULL) {
      delete reinterpret_cast<XattrList *>(stat->cvm_xattrs);
    }
    stat->cvm_xattrs = new_xattrs;
  }
  EXPECT_EQ(0, interface_->set_meta(interface_->context_, "/abc/foo", stat));
  // Just to make sure stat call actually changes the value
  stat->st_mode = 0;
  cvmfs_attr_free(stat);
  stat = cvmfs_attr_init();
  EXPECT_EQ(
      0, interface_->get_stat(interface_->context_, "/abc/foo", stat, false));

  EXPECT_EQ(mode_t(0600), (~S_IFMT) & stat->st_mode);
  if (supports_xattrs) {
    XattrList *res_xattrs = reinterpret_cast<XattrList *>(stat->cvm_xattrs);
    std::string xattr_value_result;
    EXPECT_EQ(true, res_xattrs->Get("user.test", &xattr_value_result));
    EXPECT_STREQ("bar", xattr_value_result.c_str());
  }
  cvmfs_attr_free(stat);
}


TEST_F(T_FsPosix, GetIdentifier) {
  struct cvmfs_attr *stat = cvmfs_attr_init();
  stat->cvm_checksum = strdup("da39a3ee5e6b4b0d3255bfef95601890afd80709");
  char *res = interface_->get_identifier(interface_->context_, stat);
  shash::Any result = HashMeta(stat);
  std::string result_path = std::string(
                                "/da/39/a3ee5e6b4b0d3255bfef95601890afd80709.")
                            + result.ToString();
  EXPECT_STREQ(result_path.c_str(), res);
  free(res);
  cvmfs_attr_free(stat);
}


TEST_F(T_FsPosix, TouchLinkUnlink) {
  MkdirDeep("./TouchLinkUnlink/def/", 0744, true);
  struct cvmfs_attr *stat = cvmfs_attr_init();
  // Other checksum this time to avoid collisions during touch
  stat->cvm_checksum = strdup("da39a3ee5e6b4b0d3255bfef95601890afd8070a");
  // Avoid need of sudo rights by using current user for file creation
  stat->st_gid = getgid();
  stat->st_uid = getuid();
  stat->st_mode = 0700;
  char *ident1 = interface_->get_identifier(interface_->context_, stat);
  // Some error cases
  EXPECT_EQ(-1, interface_->do_link(interface_->context_, "/foo.txt", ident1));
  EXPECT_EQ(ENOENT, errno);
  EXPECT_EQ(-1,
            interface_->do_link(interface_->context_, "/abc/foo.txt", ident1));
  EXPECT_EQ(ENOENT, errno);
  EXPECT_EQ(-1, interface_->do_unlink(interface_->context_, "/foo.txt"));
  EXPECT_EQ(ENOENT, errno);
  EXPECT_EQ(-1, interface_->do_unlink(interface_->context_, "/def"));
#ifndef __APPLE__
  EXPECT_EQ(EISDIR, errno);
#endif

  // Check if link produces actual hardlink (same inode)
  EXPECT_EQ(0, interface_->touch(interface_->context_, stat));
  // Should succeed exactly once
  EXPECT_EQ(-1, interface_->touch(interface_->context_, stat));
  EXPECT_EQ(0, interface_->do_link(interface_->context_, "/foo.txt", ident1));
  struct stat cache_link_stat;
  struct stat link_stat;
  std::string cache_link_path1 = std::string("./.data/") + ident1;
  EXPECT_EQ(0, lstat(cache_link_path1.c_str(), &cache_link_stat));
  EXPECT_EQ(0, lstat("./TouchLinkUnlink/foo.txt", &link_stat));
  EXPECT_EQ(cache_link_stat.st_ino, link_stat.st_ino);

  // Try to rewrite link...
  stat->st_mode = 0770;
  char *ident2 = interface_->get_identifier(interface_->context_, stat);
  EXPECT_EQ(0, interface_->touch(interface_->context_, stat));
  EXPECT_EQ(0, interface_->do_link(interface_->context_, "/foo.txt", ident2));
  std::string cache_link_path2 = std::string("./.data/") + ident2;
  EXPECT_EQ(0, lstat(cache_link_path2.c_str(), &cache_link_stat));
  EXPECT_EQ(0, lstat("./TouchLinkUnlink/foo.txt", &link_stat));
  EXPECT_EQ(cache_link_stat.st_ino, link_stat.st_ino);

  // Check unlink
  EXPECT_EQ(0, interface_->do_unlink(interface_->context_, "/foo.txt"));
  EXPECT_EQ(-1, lstat("./TouchLinkUnlink/foo.txt", &link_stat));

  free(ident1);
  free(ident2);
  cvmfs_attr_free(stat);
}


TEST_F(T_FsPosix, Symlink) {
  struct cvmfs_attr *stat = cvmfs_attr_init();
  EXPECT_EQ(0, interface_->do_symlink(
                   interface_->context_, "/asymlink", "somedestination", stat));
  platform_stat64 buf;
  EXPECT_EQ(0, platform_lstat("./Symlink/asymlink", &buf));
  EXPECT_TRUE(S_ISLNK(buf.st_mode));
  char strbuf[17];
  size_t read_len = readlink("./Symlink/asymlink", strbuf, sizeof(strbuf) - 1);
  strbuf[read_len] = '\0';
  EXPECT_STREQ("somedestination", strbuf);
  EXPECT_EQ(0, interface_->do_symlink(
                   interface_->context_, "/asymlink", "anotherdestinat", stat));
  read_len = readlink("./Symlink/asymlink", strbuf, sizeof(strbuf) - 1);
  strbuf[read_len] = '\0';
  EXPECT_STREQ("anotherdestinat", strbuf);
  cvmfs_attr_free(stat);
}


TEST_F(T_FsPosix, ReadWrite) {
  std::string ident = "/ff/ff/test.txt";
  std::string path = "./.data/" + ident;
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
  EXPECT_EQ(0, interface_->do_fopen(handle, fs_open_write));
  EXPECT_EQ(-1, interface_->do_fread(handle, dummybuf, 1, &read_len));
  EXPECT_EQ(0, interface_->do_fwrite(handle, "foo", 3));
  EXPECT_EQ(0, interface_->do_fclose(handle));

  // Read (check) and append with posix (content should be foo)
  fd = fopen(path.c_str(), "r+");
  EXPECT_EQ(3U, fread(buf1, sizeof(char), 3, fd));
  buf1[3] = '\0';
  EXPECT_STREQ("foo", buf1);
  EXPECT_EQ(3U, fwrite("bar", sizeof(char), 3, fd));
  fclose(fd);

  // Read with interface (content should be foobar)
  EXPECT_EQ(0, interface_->do_fopen(handle, fs_open_read));
  EXPECT_EQ(-1, interface_->do_fwrite(handle, "foo", 3));
  EXPECT_EQ(0, interface_->do_fread(handle, buf2, 6, &read_len));
  buf2[6] = '\0';
  EXPECT_STREQ("foobar", buf2);
  EXPECT_EQ(0, interface_->do_fclose(handle));

  // Append with interface
  EXPECT_EQ(0, interface_->do_fopen(handle, fs_open_append));
  EXPECT_EQ(-1, interface_->do_fread(handle, dummybuf, 1, &read_len));
  EXPECT_EQ(0, interface_->do_fwrite(handle, "bar", 3));
  EXPECT_EQ(0, interface_->do_fclose(handle));

  // Read (check) with posix (content should be foobarbar)
  fd = fopen(path.c_str(), "r");
  EXPECT_EQ(9U, fread(buf3, sizeof(char), 9, fd));
  buf3[9] = '\0';
  EXPECT_STREQ("foobarbar", buf3);
  fclose(fd);

  interface_->do_ffree(handle);
}


TEST_F(T_FsPosix, MkDirRmDir) {
  MkdirDeep("./MkDirRmDir/abc/", 0744, true);
  MkdirDeep("./MkDirRmDir/abc/def", 0744, true);
  struct cvmfs_attr *stat = cvmfs_attr_init();
  struct stat buf;
  stat->st_gid = getgid();
  stat->st_uid = getuid();
  stat->st_mode = 0700;

  // Check a few cases which should fail
  EXPECT_EQ(-1, interface_->do_mkdir(interface_->context_, "/def/ghi", stat));
  EXPECT_EQ(ENOENT, errno);
  EXPECT_EQ(-1, interface_->do_rmdir(interface_->context_, "/def"));
  EXPECT_EQ(ENOENT, errno);
  EXPECT_EQ(-1, interface_->do_rmdir(interface_->context_, "/abc"));
  EXPECT_EQ(ENOTEMPTY, errno);

  // Try to rewrite a directory
  EXPECT_EQ(0, interface_->do_mkdir(interface_->context_, "/abc", stat));
  EXPECT_EQ(0, lstat("./MkDirRmDir/abc", &buf));
  EXPECT_EQ(mode_t(0700), ((~S_IFMT) & stat->st_mode));
  EXPECT_EQ(-1, lstat("./MkDirRmDir/abc/def", &buf));

  // Try to create simple new directory
  EXPECT_EQ(0, interface_->do_mkdir(interface_->context_, "/abc/def", stat));
  EXPECT_EQ(0, lstat("./MkDirRmDir/abc", &buf));
  // Delete them again
  EXPECT_EQ(0, interface_->do_rmdir(interface_->context_, "/abc/def"));
  EXPECT_EQ(-1, lstat("./MkDirRmDir/abc/def", &buf));
  EXPECT_EQ(0, interface_->do_rmdir(interface_->context_, "/abc"));
  EXPECT_EQ(-1, lstat("./MkDirRmDir/abc", &buf));

  cvmfs_attr_free(stat);
}


TEST_F(T_FsPosix, HasFile) {
  CreateFile("./.data/ff/ff/test.txt", 0744, false);
  EXPECT_TRUE(interface_->has_file(interface_->context_, "/ff/ff/test.txt"));
  EXPECT_FALSE(
      interface_->has_file(interface_->context_, "/ff/ff/test.txt.notavail"));
}


TEST_F(T_FsPosix, GarbageCollection) {
  struct fs_traversal *dest = posix_get_interface();
  struct fs_traversal_context *context = dest->initialize("./", "posix",
                                                          "./data", NULL, 4);
  const bool supports_xattrs = SupportsXattrs(".");

  std::string content1 = "a";
  shash::Any content1_hash(shash::kSha1);
  shash::HashString(content1, &content1_hash);
  std::string content2 = "b";
  shash::Any content2_hash(shash::kSha1);
  shash::HashString(content2, &content2_hash);
  XattrList *xlist1 = NULL;
  XattrList *xlist2 = NULL;
  XattrList *xlist11 = NULL;
  XattrList *xlist22 = NULL;
  if (supports_xattrs) {
    xlist1 = CreateSampleXattrlist("TestGarbageCollection1");
    xlist2 = CreateSampleXattrlist("TestGarbageCollection2");
    xlist11 = CreateSampleXattrlist("TestGarbageCollection1");
    xlist22 = CreateSampleXattrlist("TestGarbageCollection2");
  }
  struct cvmfs_attr *stat1 = CreateSampleStat("foo", 0, 0777, 0, xlist1,
                                              &content1_hash);
  struct cvmfs_attr *stat2 = CreateSampleStat("foo", 0, 0777, 0, xlist11,
                                              &content2_hash);
  struct cvmfs_attr *stat3 = CreateSampleStat("foo", 0, 0777, 0, xlist2,
                                              &content1_hash);
  struct cvmfs_attr *stat4 = CreateSampleStat("foo", 0, 0777, 0, xlist22,
                                              &content2_hash);

  EXPECT_EQ(0, dest->touch(context, stat1));
  char *ident1 = dest->get_identifier(context, stat1);
  dest->do_link(context, "file1.txt", ident1);
  EXPECT_EQ(0, dest->touch(context, stat2));
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
  if (supports_xattrs) {
    EXPECT_STRNE(ident1, ident3);
    EXPECT_STRNE(ident2, ident4);
  }
  EXPECT_STRNE(ident1, ident2);
  EXPECT_STRNE(ident1, ident4);
  EXPECT_STRNE(ident2, ident3);
  EXPECT_STRNE(ident3, ident4);
  EXPECT_FALSE(FileExists(data_base_path + ident1));
  EXPECT_TRUE(FileExists(data_base_path + ident2));
  EXPECT_FALSE(FileExists(data_base_path + ident3));
  EXPECT_TRUE(FileExists(data_base_path + ident4));
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
