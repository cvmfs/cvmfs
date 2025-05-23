/**
 * This file is part of the CernVM File System.
 */

#include <errno.h>
#include <gtest/gtest.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "libcvmfs.h"
#include "shrinkwrap/fs_traversal.h"
#include "shrinkwrap/fs_traversal_interface.h"
#include "shrinkwrap/fs_traversal_libcvmfs.h"
#include "shrinkwrap/posix/interface.h"
#include "shrinkwrap/util.h"
#include "statistics.h"
#include "testutil_shrinkwrap.h"
#include "util/platform.h"
#include "util/posix.h"
#include "xattr.h"

const mode_t kModeBitmask = (S_IRWXO | S_IRWXG | S_IRWXU);

using namespace std;  // NOLINT

struct FsTest {
  FsTest() : interface(posix_get_interface()), repo("."), data("./data") { }
  struct fs_traversal *interface;
  const char *repo;
  const char *data;
};

class T_FsInterface : public ::testing::Test {
 protected:
  virtual void SetUp() { Init(); }

  virtual void TearDown() {
    // TODO(steuber): Where to free?
    Fin();
    // delete fs_instance_->interface;
  }

  void Init() {
    fs_instance_ = new FsTest();
    const char *repo = fs_instance_->repo;
    if (repo == NULL) {
      // Current working directory setup from testing environment
      repo = "./";
    }
    context_ = fs_instance_->interface->initialize(repo, repo,
                                                   fs_instance_->data, NULL, 4);
    fs_instance_->interface->context_ = context_;
    supports_xattrs_ = SupportsXattrs(".");
  }

  void Fin() {
    fs_instance_->interface->finalize(context_);
    context_ = NULL;
  }

  void Restart() {
    Fin();
    Init();
  }

  /**
   * Creates the following entries:
   * DIRECTORIES:
   * /[prefix]-foo
   * /[prefix]-foo/bar
   * /[prefix]-bar
   * /[prefix]-bar/foobar
   * /[prefix]-bar/foobar/foobar
   * /[prefix]-bar/test
   *
   * FILES:
   * /[prefix]-foo.txt -> ident1
   * /[prefix]-bar.txt -> ident2
   * /[prefix]-foo/bar/foobar1.txt -> ident1
   * /[prefix]-foo/bar/foobar2.txt -> ident1
   * /[prefix]-foo/bar/foobar3.txt -> ident1
   * /[prefix]-foo/bar/foobar3.txt -> ident1
   * /[prefix]-foo/bar/foobar3.txt -> ident1
   *
   * SYMLINKS:
   * ./[prefix]-symlink1 -> ./foo
   * ./[prefix]-foo/bar/symlink2 -> ./foobar
   */
  void MakeTestFiles(std::string prefix,
                     std::string *ident1,
                     std::string *ident2) {
    // File Content1
    std::string content1 = prefix + ": Hello world!\nHello traversal!";
    shash::Any content1_hash(shash::kSha1);
    shash::HashString(content1, &content1_hash);

    // File Content2
    std::string content2 = prefix + ": Hello traversal!\nHello world!";
    shash::Any content2_hash(shash::kSha1);
    shash::HashString(content2, &content2_hash);

    // File meta data 1
    XattrList *xlistdir = NULL;
    XattrList *xlist1 = NULL;
    XattrList *xlist2 = NULL;
    if (supports_xattrs_) {
      xlistdir = CreateSampleXattrlist(prefix);
      xlist1 = CreateSampleXattrlist(prefix);
      xlist2 = CreateSampleXattrlist(prefix + "-2");
    }

    struct cvmfs_attr *stat_values_dir = CreateSampleStat(
        ("/" + prefix + "-bar.txt").c_str(), 10, 0770, 0, xlistdir,
        &content1_hash);
    struct cvmfs_attr *stat_values1 = CreateSampleStat(
        ("/" + prefix + "-foo.txt").c_str(), 10, 0770, 0, xlist1,
        &content1_hash);
    struct cvmfs_attr *stat_values2 = CreateSampleStat(
        ("/" + prefix + "-bar.txt").c_str(), 10, 0777, 0, xlist2,
        &content2_hash);
    char *buf = fs_instance_->interface->get_identifier(context_, stat_values1);
    *ident1 = std::string(buf);
    free(buf);
    buf = fs_instance_->interface->get_identifier(context_, stat_values2);
    *ident2 = std::string(buf);
    free(buf);

    // Background Files
    ASSERT_EQ(0, fs_instance_->interface->touch(context_, stat_values1));
    void *hdl1 = fs_instance_->interface->get_handle(context_, ident1->c_str());
    fs_instance_->interface->do_fopen(hdl1, fs_open_write);
    fs_instance_->interface->do_fwrite(hdl1, content1.c_str(),
                                       content1.length());
    fs_instance_->interface->do_fclose(hdl1);
    fs_instance_->interface->do_ffree(hdl1);
    ASSERT_EQ(0, fs_instance_->interface->touch(context_, stat_values2));
    void *hdl2 = fs_instance_->interface->get_handle(context_, ident2->c_str());
    fs_instance_->interface->do_fopen(hdl2, fs_open_write);
    fs_instance_->interface->do_fwrite(hdl2, content2.c_str(),
                                       content2.length());
    fs_instance_->interface->do_fclose(hdl2);
    fs_instance_->interface->do_ffree(hdl2);
    // Directories
    ASSERT_EQ(0,
              fs_instance_->interface->do_mkdir(
                  context_, ("/" + prefix + "-foo").c_str(), stat_values_dir));
    ASSERT_EQ(0,
              fs_instance_->interface->do_mkdir(
                  context_, ("/" + prefix + "-bar").c_str(), stat_values_dir));
    ASSERT_EQ(
        0,
        fs_instance_->interface->do_mkdir(
            context_, ("/" + prefix + "-bar/test").c_str(), stat_values_dir));
    ASSERT_EQ(
        0, fs_instance_->interface->do_mkdir(
               context_, ("/" + prefix + "-foo/bar").c_str(), stat_values_dir));
    ASSERT_EQ(
        0,
        fs_instance_->interface->do_mkdir(
            context_, ("/" + prefix + "-bar/foobar").c_str(), stat_values_dir));
    ASSERT_EQ(0, fs_instance_->interface->do_mkdir(
                     context_,
                     ("/" + prefix + "-bar/foobar/foobar").c_str(),
                     stat_values_dir));

    // Files
    ASSERT_EQ(
        0, fs_instance_->interface->do_link(
               context_, ("/" + prefix + "-foo.txt").c_str(), ident1->c_str()));
    ASSERT_EQ(
        0, fs_instance_->interface->do_link(
               context_, ("/" + prefix + "-bar.txt").c_str(), ident2->c_str()));
    ASSERT_EQ(0, fs_instance_->interface->do_link(
                     context_,
                     ("/" + prefix + "-foo/bar/foobar1.txt").c_str(),
                     ident1->c_str()));
    ASSERT_EQ(0, fs_instance_->interface->do_link(
                     context_,
                     ("/" + prefix + "-foo/bar/foobar2.txt").c_str(),
                     ident1->c_str()));
    ASSERT_EQ(0, fs_instance_->interface->do_link(
                     context_,
                     ("/" + prefix + "-foo/bar/foobar3.txt").c_str(),
                     ident1->c_str()));

    // Symlinks
    ASSERT_EQ(0, fs_instance_->interface->do_symlink(
                     context_,
                     ("/" + prefix + "-symlink1").c_str(),
                     "./foo",
                     stat_values1));
    ASSERT_EQ(0, fs_instance_->interface->do_symlink(
                     context_,
                     ("/" + prefix + "-foo/bar/symlink2").c_str(),
                     "./foobar",
                     stat_values1));

    // Check hash consistency
    ASSERT_TRUE(
        fs_instance_->interface->is_hash_consistent(context_, stat_values1));
    ASSERT_TRUE(
        fs_instance_->interface->is_hash_consistent(context_, stat_values2));
    ASSERT_FALSE(
        fs_instance_->interface->is_hash_consistent(context_, stat_values_dir));
    ASSERT_EQ(
        0, fs_instance_->interface->do_link(
               context_, ("/" + prefix + "-foo.txt").c_str(), ident2->c_str()));
    ASSERT_FALSE(
        fs_instance_->interface->is_hash_consistent(context_, stat_values1));
    ASSERT_EQ(
        0, fs_instance_->interface->do_link(
               context_, ("/" + prefix + "-foo.txt").c_str(), ident1->c_str()));

    cvmfs_attr_free(stat_values1);
    cvmfs_attr_free(stat_values2);
    cvmfs_attr_free(stat_values_dir);
    // Check if fs is consistent over finalizing and reopening...
    Restart();
  }

 protected:
  struct FsTest *fs_instance_;
  struct fs_traversal_context *context_;
  bool supports_xattrs_;
};


TEST_F(T_FsInterface, Stat) {
  std::string ident1;
  std::string ident2;
  MakeTestFiles("Stat", &ident1, &ident2);
  struct cvmfs_attr *foostat = cvmfs_attr_init();
  struct cvmfs_attr *barstat = cvmfs_attr_init();
  struct cvmfs_attr *symlinkstat = cvmfs_attr_init();
  std::string val;
  // Correct inode configuration
  EXPECT_EQ(0, fs_instance_->interface->get_stat(context_, "/Stat-foo.txt",
                                                 foostat, false));
  EXPECT_STREQ("Stat-foo.txt", foostat->cvm_name);
  EXPECT_STREQ("", foostat->cvm_parent);
  EXPECT_EQ(mode_t(0770), kModeBitmask & foostat->st_mode);
  EXPECT_EQ(getuid(), foostat->st_uid);
  EXPECT_EQ(getgid(), foostat->st_gid);
  cvmfs_attr_free(foostat);
  foostat = cvmfs_attr_init();
  EXPECT_EQ(0, fs_instance_->interface->get_stat(
                   context_, "/Stat-foo/bar/foobar1.txt", foostat, false));
  EXPECT_STREQ("foobar1.txt", foostat->cvm_name);
  EXPECT_STREQ("/Stat-foo/bar", foostat->cvm_parent);

  XattrList *xlist1 = reinterpret_cast<XattrList *>(foostat->cvm_xattrs);
  xlist1->Get("user.foo", &val);
  if (supports_xattrs_)
    EXPECT_STREQ("Stat", val.c_str());
  else
    EXPECT_TRUE(val.empty());

  EXPECT_EQ(0, fs_instance_->interface->get_stat(context_, "/Stat-bar.txt",
                                                 barstat, false));
  XattrList *xlist2 = reinterpret_cast<XattrList *>(barstat->cvm_xattrs);
  xlist2->Get("user.foo", &val);
  if (supports_xattrs_)
    EXPECT_STREQ("Stat-2", val.c_str());
  else
    EXPECT_TRUE(val.empty());

  EXPECT_EQ(0, fs_instance_->interface->get_stat(context_, "/Stat-symlink1",
                                                 symlinkstat, false));
  EXPECT_STREQ("./foo", symlinkstat->cvm_symlink);

  cvmfs_attr_free(foostat);
  cvmfs_attr_free(barstat);
  cvmfs_attr_free(symlinkstat);
}


TEST_F(T_FsInterface, Touch) {
  std::string ident1;
  std::string ident2;
  MakeTestFiles("Touch", &ident1, &ident2);

  // FILE CONTENT 1
  std::string content1 = "Touch: Hello world!\nHello traversal!";
  shash::Any content1_hash(shash::kSha1);
  shash::HashString(content1, &content1_hash);
  std::string content2 = "Touch: Hello traversal!\nHello world!";
  shash::Any content2_hash(shash::kSha1);
  shash::HashString(content2, &content2_hash);
  // FILE META 3
  XattrList *xlist3 = NULL;
  XattrList *xlist4 = NULL;
  XattrList *xlist5 = NULL;
  if (supports_xattrs_) {
    xlist3 = CreateSampleXattrlist("Touch: foo");
    xlist4 = CreateSampleXattrlist("Touch: foobarfoo");
    xlist5 = new XattrList(*xlist4);
  }

  struct cvmfs_attr *stat_values3 = CreateSampleStat(
      "hello.world", 10, 0700, content1.length(), xlist3, &content1_hash);

  struct cvmfs_attr *stat_values4 = CreateSampleStat(
      "hello.world", 10, 0700, content1.length(), xlist4, &content1_hash);
  char *buf = fs_instance_->interface->get_identifier(context_, stat_values4);
  std::string ident4 = std::string(buf);
  free(buf);

  struct cvmfs_attr *stat_values5 = CreateSampleStat(
      "hello.world", 10, 0700, content1.length(), xlist5, &content2_hash);
  buf = fs_instance_->interface->get_identifier(context_, stat_values5);
  std::string ident5 = std::string(buf);
  free(buf);

  // Check directory listings
  size_t listLen = 0;
  char **dirList;
  fs_instance_->interface->list_dir(context_, "/", &dirList, &listLen);
  ExpectListHas("Touch-foo.txt", dirList);
  ExpectListHas("Touch-bar.txt", dirList);
  FreeList(dirList, listLen);
  listLen = 0;
  fs_instance_->interface->list_dir(
      context_, "/Touch-foo/bar/", &dirList, &listLen);
  ExpectListHas("foobar1.txt", dirList);
  ExpectListHas("foobar2.txt", dirList);
  ExpectListHas("foobar3.txt", dirList);
  FreeList(dirList, listLen);
  listLen = 0;

  // Creating again should fail...
  EXPECT_EQ(0, fs_instance_->interface->touch(context_, stat_values3));
  EXPECT_EQ(-1, fs_instance_->interface->touch(context_, stat_values3));
  // With errno...
  EXPECT_EQ(EEXIST, errno);
  EXPECT_TRUE(fs_instance_->interface->has_file(context_, ident1.c_str()));
  EXPECT_TRUE(fs_instance_->interface->has_file(context_, ident2.c_str()));
  if (supports_xattrs_) {
    EXPECT_FALSE(fs_instance_->interface->has_file(context_, ident4.c_str()));
  }
  EXPECT_FALSE(fs_instance_->interface->has_file(context_, ident5.c_str()));
  cvmfs_attr_free(stat_values3);
  cvmfs_attr_free(stat_values4);
  cvmfs_attr_free(stat_values5);
}


TEST_F(T_FsInterface, MkRmDir) {
  std::string ident1;
  std::string ident2;
  MakeTestFiles("MkRmDir", &ident1, &ident2);
  XattrList *xlist1 = NULL;
  if (supports_xattrs_)
    xlist1 = CreateSampleXattrlist("MkRmDir: foo");
  struct cvmfs_attr *stat_values_dir = CreateSampleStat("MkRmDir-hello.world",
                                                        10, 0770, 0, xlist1);
  // Insert in non existing parent directory
  EXPECT_EQ(-1, fs_instance_->interface->do_mkdir(
                    context_, "/MkRmDir-foo/foobar/foobar", stat_values_dir));
  EXPECT_EQ(ENOENT, errno);
  // Check directory listings
  size_t listLen = 0;
  char **dirList;
  fs_instance_->interface->list_dir(
      context_, "/MkRmDir-bar", &dirList, &listLen);
  EXPECT_EQ(2U, listLen);
  ExpectListHas("foobar", dirList);
  ExpectListHas("test", dirList);
  FreeList(dirList, listLen);
  listLen = 0;
  EXPECT_EQ(-1, fs_instance_->interface->do_rmdir(
                    context_, "/MkRmDir-foo/foobar/foobar"));
  EXPECT_EQ(ENOENT, errno);
  // No recursive deletion
  EXPECT_EQ(-1,
            fs_instance_->interface->do_rmdir(context_, "/MkRmDir-bar/foobar"));
  EXPECT_EQ(0, fs_instance_->interface->do_rmdir(context_,
                                                 "/MkRmDir-bar/foobar/foobar"));
  EXPECT_EQ(0,
            fs_instance_->interface->do_rmdir(context_, "/MkRmDir-bar/foobar"));
  fs_instance_->interface->list_dir(
      context_, "/MkRmDir-bar", &dirList, &listLen);
  EXPECT_EQ(1U, listLen);
  ExpectListHas("foobar", dirList, true);
  ExpectListHas("test", dirList);
  FreeList(dirList, listLen);
  listLen = 0;
  EXPECT_EQ(-1,
            fs_instance_->interface->do_rmdir(context_, "/MkRmDir-bar/foobar"));
  EXPECT_EQ(ENOENT, errno);
  cvmfs_attr_free(stat_values_dir);
}


TEST_F(T_FsInterface, ListDir) {
  std::string ident1;
  std::string ident2;
  MakeTestFiles("ListDir", &ident1, &ident2);
  size_t listLen = 0;
  char **dirList;
  fs_instance_->interface->list_dir(context_, "/", &dirList, &listLen);
  ExpectListHas("ListDir-foo", dirList);
  ExpectListHas("ListDir-bar", dirList);
  ExpectListHas("ListDir-foo.txt", dirList);
  ExpectListHas("ListDir-bar.txt", dirList);
  ExpectListHas("ListDir-symlink1", dirList);
  FreeList(dirList, listLen);
  listLen = 0;
  fs_instance_->interface->list_dir(
      context_, "/ListDir-foo", &dirList, &listLen);
  EXPECT_EQ(1U, listLen);
  ExpectListHas("bar", dirList);
  FreeList(dirList, listLen);
  listLen = 0;
  fs_instance_->interface->list_dir(
      context_, "/ListDir-foo/bar", &dirList, &listLen);
  EXPECT_EQ(4U, listLen);
  ExpectListHas("foobar1.txt", dirList);
  ExpectListHas("foobar2.txt", dirList);
  ExpectListHas("foobar3.txt", dirList);
  ExpectListHas("symlink2", dirList);
  FreeList(dirList, listLen);
  listLen = 0;
  fs_instance_->interface->list_dir(
      context_, "/ListDir-bar", &dirList, &listLen);
  EXPECT_EQ(2U, listLen);
  ExpectListHas("foobar", dirList, true);
  ExpectListHas("test", dirList);
  FreeList(dirList, listLen);
  listLen = 0;
  fs_instance_->interface->list_dir(
      context_, "/ListDir-bar/foobar", &dirList, &listLen);
  ExpectListHas("foobar", dirList);
  FreeList(dirList, listLen);
  listLen = 0;
}


TEST_F(T_FsInterface, ReadWrite) {
  std::string ident1;
  std::string ident2;
  MakeTestFiles("ReadWrite", &ident1, &ident2);
  void *hdl1 = fs_instance_->interface->get_handle(context_, ident1.c_str());
  ASSERT_TRUE(NULL != hdl1);
  EXPECT_EQ(0, fs_instance_->interface->do_fopen(hdl1, fs_open_write));
  std::string content1 = "Lorem ipsum dolor sit amet.";
  EXPECT_EQ(0,
            fs_instance_->interface->do_fwrite(hdl1, content1.c_str(),
                                               content1.length()));
  char buf[50];
  size_t rlen;
  EXPECT_EQ(-1, fs_instance_->interface->do_fread(hdl1, buf, 100, &rlen));
  EXPECT_EQ(0, fs_instance_->interface->do_fclose(hdl1));
  EXPECT_EQ(0, fs_instance_->interface->do_fopen(hdl1, fs_open_read));
  EXPECT_EQ(-1,
            fs_instance_->interface->do_fwrite(hdl1, content1.c_str(),
                                               content1.length()));
  EXPECT_EQ(0, fs_instance_->interface->do_fread(hdl1, buf, 100, &rlen));
  buf[rlen] = '\0';
  EXPECT_EQ(content1.length(), rlen);
  EXPECT_STREQ(content1.c_str(), buf);
  EXPECT_EQ(0, fs_instance_->interface->do_fclose(hdl1));
  fs_instance_->interface->do_ffree(hdl1);
}


TEST_F(T_FsInterface, LinkUnlink) {
  std::string ident1;
  std::string ident2;
  MakeTestFiles("LinkUnlink", &ident1, &ident2);
  struct cvmfs_attr *foostat = cvmfs_attr_init();
  struct cvmfs_attr *barstat = cvmfs_attr_init();
  struct cvmfs_attr *foobarstat = cvmfs_attr_init();
  // Correct inode configuration
  EXPECT_EQ(0, fs_instance_->interface->get_stat(
                   context_, "/LinkUnlink-foo.txt", foostat, false));
  EXPECT_EQ(0, fs_instance_->interface->get_stat(
                   context_, "/LinkUnlink-bar.txt", barstat, false));
  EXPECT_NE(foostat->st_ino, barstat->st_ino);
  EXPECT_EQ(
      0, fs_instance_->interface->get_stat(
             context_, "/LinkUnlink-foo/bar/foobar1.txt", foobarstat, false));
  EXPECT_EQ(foostat->st_ino, foobarstat->st_ino);
  cvmfs_attr_free(foobarstat);
  foobarstat = cvmfs_attr_init();
  EXPECT_EQ(
      0, fs_instance_->interface->get_stat(
             context_, "/LinkUnlink-foo/bar/foobar2.txt", foobarstat, false));
  EXPECT_EQ(foostat->st_ino, foobarstat->st_ino);
  cvmfs_attr_free(foobarstat);
  foobarstat = cvmfs_attr_init();
  EXPECT_EQ(
      0, fs_instance_->interface->get_stat(
             context_, "/LinkUnlink-foo/bar/foobar3.txt", foobarstat, false));
  EXPECT_EQ(foostat->st_ino, foobarstat->st_ino);
  cvmfs_attr_free(foobarstat);
  foobarstat = cvmfs_attr_init();
  EXPECT_EQ(-1, fs_instance_->interface->do_link(
                    context_, "/LinkUnlink-foobar/foofoobar/foofoofoobar",
                    ident1.c_str()));
  EXPECT_EQ(ENOENT, errno);
  EXPECT_EQ(-1, fs_instance_->interface->do_link(
                    context_, "/LinkUnlink-foobar.txt", "foobarfoobar"));
  EXPECT_EQ(ENOENT, errno);
  EXPECT_EQ(0,
            fs_instance_->interface->do_link(
                context_, "/LinkUnlink-foo/bar/foobar3.txt", ident2.c_str()));
  EXPECT_EQ(
      0, fs_instance_->interface->get_stat(
             context_, "/LinkUnlink-foo/bar/foobar3.txt", foobarstat, false));
  EXPECT_EQ(barstat->st_ino, foobarstat->st_ino);

  cvmfs_attr_free(foostat);
  cvmfs_attr_free(barstat);
  cvmfs_attr_free(foobarstat);

  // Correct unlink behaviour
  EXPECT_EQ(-1, fs_instance_->interface->do_unlink(
                    context_, "/LinkUnlink-foobar/foofoobar/foofoofoobar"));
  EXPECT_EQ(ENOENT, errno);
  EXPECT_EQ(-1, fs_instance_->interface->do_unlink(
                    context_, "/LinkUnlink-bar/foobar/foobar"));
#ifndef __APPLE__
  EXPECT_EQ(EISDIR, errno);
#endif
}


TEST_F(T_FsInterface, Symlink) {
  std::string ident1;
  std::string ident2;
  MakeTestFiles("Symlink", &ident1, &ident2);
  struct cvmfs_attr *sl1Stat = cvmfs_attr_init();
  struct cvmfs_attr *sl2Stat = cvmfs_attr_init();
  EXPECT_EQ(0, fs_instance_->interface->get_stat(context_, "/Symlink-symlink1",
                                                 sl1Stat, false));
  EXPECT_STREQ("./foo", sl1Stat->cvm_symlink);
  EXPECT_EQ(0, fs_instance_->interface->get_stat(
                   context_, "/Symlink-foo/bar/symlink2", sl2Stat, false));
  EXPECT_STREQ("./foobar", sl2Stat->cvm_symlink);
  cvmfs_attr_free(sl1Stat);
  cvmfs_attr_free(sl2Stat);
}


TEST_F(T_FsInterface, TransferPosixToPosix) {
  std::string ident1;
  std::string ident2;
  std::string prefix = "SRC";
  MakeTestFiles(prefix, &ident1, &ident2);

  std::string repo_name = GetCurrentWorkingDirectory();
  std::string src_name = "/SRC-foo";
  std::string dest_name = "/SRC-bar";
  std::string dest_data = repo_name + "/SRC-DDATA";

  struct fs_traversal *src = posix_get_interface();
  struct fs_traversal_context *context = src->initialize(
      src_name.c_str(), repo_name.c_str(), NULL, NULL, 4);
  src->context_ = context;

  struct fs_traversal *dest = posix_get_interface();
  context = dest->initialize(src_name.c_str(), repo_name.c_str(), NULL, NULL,
                             4);
  dest->context_ = context;

  perf::Statistics *statistics = shrinkwrap::GetSyncStatTemplate();

  EXPECT_TRUE(
      shrinkwrap::SyncFull(src, dest, statistics, platform_monotonic_time()));

  dest->finalize(dest->context_);
  context = dest->initialize(dest_name.c_str(), repo_name.c_str(),
                             dest_data.c_str(), NULL, 4);
  dest->context_ = context;

  EXPECT_TRUE(
      shrinkwrap::SyncFull(src, dest, statistics, platform_monotonic_time()));
  std::string srcdir = repo_name + "/" + src_name;
  std::string destdir = repo_name + "/" + dest_name;
  EXPECT_TRUE(DiffTree(srcdir, destdir));

  src->finalize(src->context_);
  dest->finalize(dest->context_);
  delete statistics;
  delete src;
  delete dest;
}
