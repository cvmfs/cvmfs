/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <ctime>
#include <iostream>

#include "libcvmfs.h"
#include "statistics.h"
#include "util/posix.h"
#include "xattr.h"

#include "shrinkwrap/fs_traversal.h"
#include "shrinkwrap/fs_traversal_interface.h"
#include "shrinkwrap/fs_traversal_libcvmfs.h"
#include "shrinkwrap/posix/interface.h"
#include "shrinkwrap/util.h"
#include "test-util.h"

#define MODE_BITMASK (S_IRWXO | S_IRWXG | S_IRWXU)

using namespace std;  // NOLINT

struct fs_traversal_test {
  struct fs_traversal *interface;
  const char *repo;
  const char *data;
};

class T_Fs_Traversal_Interface :
  public ::testing::TestWithParam<struct fs_traversal_test *> {
 protected:
  virtual void SetUp() {
    Init();
  }

  virtual void TearDown() {
    // TODO(steuber): Where to free?
    Fin();
    // delete fs_traversal_instance_->interface;
  }

  void Init() {
    fs_traversal_instance_ = GetParam();
    const char *repo = fs_traversal_instance_->repo;
    if (repo == NULL) {
      // Current working directory setup from testing environment
      repo = "./";
    }
    context_ = fs_traversal_instance_->interface->initialize(
      repo,
      repo,
      fs_traversal_instance_->data, NULL, 4);
    fs_traversal_instance_->interface->context_ = context_;
  }

  void Fin() {
    fs_traversal_instance_->interface->finalize(context_);
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
    std::string *ident1, std::string *ident2) {
    // FILE CONTENT 1
    std::string content1 = prefix + ": Hello world!\nHello traversal!";
    shash::Any content1_hash(shash::kSha1);
    shash::HashString(content1, &content1_hash);

    FILE *f = fopen("temp.data", "w");
    if (f == NULL) {
      perror("Open failed :");
    }
    fwrite(content1.c_str(), sizeof(char), content1.length(), f);
    fclose(f);
    shash::Any cvm_checksum = shash::Any(shash::kSha1);
    shash::HashFile("temp.data", &cvm_checksum);
    ASSERT_STREQ(cvm_checksum.ToString().c_str(),
                 content1_hash.ToString().c_str());

    // FILE CONTENT 2
    std::string content2 = prefix + ": Hello traversal!\nHello world!";
    shash::Any content2_hash(shash::kSha1);
    shash::HashString(content2, &content2_hash);
    // FILE META 1
    XattrList *xlistdir = create_sample_xattrlist(prefix);
    struct cvmfs_attr *stat_values_dir = create_sample_stat(
      ("/" + prefix + "-bar.txt").c_str(),
      10, 0770, 0, xlistdir, &content1_hash);
    XattrList *xlist1 = create_sample_xattrlist(prefix);
    struct cvmfs_attr *stat_values1 = create_sample_stat(
      ("/" + prefix + "-foo.txt").c_str(),
      10, 0770, 0, xlist1, &content1_hash);
    XattrList *xlist2 = create_sample_xattrlist(prefix+"-2");
    struct cvmfs_attr *stat_values2 = create_sample_stat(
      ("/" + prefix + "-bar.txt").c_str(),
      10, 0777, 0, xlist2, &content2_hash);
    char *buf = fs_traversal_instance_->interface->get_identifier(context_,
      stat_values1);
    *ident1 = std::string(buf);
    free(buf);
    buf = fs_traversal_instance_->interface->get_identifier(context_,
      stat_values2);
    *ident2 = std::string(buf);
    free(buf);

    // BACKGROUND FILES
    ASSERT_EQ(0, fs_traversal_instance_->interface->touch(
      context_,
      stat_values1));
    void *hdl1 = fs_traversal_instance_->interface->get_handle(
      context_, ident1->c_str());
    fs_traversal_instance_->interface->do_fopen(hdl1, fs_open_write);
    fs_traversal_instance_->interface->do_fwrite(hdl1, content1.c_str(),
      content1.length());
    fs_traversal_instance_->interface->do_fclose(hdl1);
    fs_traversal_instance_->interface->do_ffree(hdl1);
    ASSERT_EQ(0, fs_traversal_instance_->interface->touch(
      context_,
      stat_values2));
    void *hdl2 = fs_traversal_instance_->interface->get_handle(
      context_, ident2->c_str());
    fs_traversal_instance_->interface->do_fopen(hdl2, fs_open_write);
    fs_traversal_instance_->interface->do_fwrite(hdl2, content2.c_str(),
      content2.length());
    fs_traversal_instance_->interface->do_fclose(hdl2);
    fs_traversal_instance_->interface->do_ffree(hdl2);
    // DIRECTORIES
    ASSERT_EQ(0, fs_traversal_instance_->interface->do_mkdir(
      context_,
      ("/" + prefix + "-foo").c_str(),
      stat_values_dir));
    ASSERT_EQ(0, fs_traversal_instance_->interface->do_mkdir(
      context_,
      ("/" + prefix + "-bar").c_str(),
      stat_values_dir));
    ASSERT_EQ(0, fs_traversal_instance_->interface->do_mkdir(
      context_,
      ("/" + prefix + "-bar/test").c_str(),
      stat_values_dir));
    ASSERT_EQ(0, fs_traversal_instance_->interface->do_mkdir(
      context_,
      ("/" + prefix + "-foo/bar").c_str(),
      stat_values_dir));
    ASSERT_EQ(0, fs_traversal_instance_->interface->do_mkdir(
      context_,
      ("/" + prefix + "-bar/foobar").c_str(),
      stat_values_dir));
    ASSERT_EQ(0, fs_traversal_instance_->interface->do_mkdir(
      context_,
      ("/" + prefix + "-bar/foobar/foobar").c_str(),
      stat_values_dir));

    // FILES
    ASSERT_EQ(0, fs_traversal_instance_->interface->do_link(
      context_,
      ("/" + prefix + "-foo.txt").c_str(),
      ident1->c_str()));
    ASSERT_EQ(0, fs_traversal_instance_->interface->do_link(
      context_,
      ("/" + prefix + "-bar.txt").c_str(),
      ident2->c_str()));
    ASSERT_EQ(0, fs_traversal_instance_->interface->do_link(
      context_,
      ("/" + prefix + "-foo/bar/foobar1.txt").c_str(),
      ident1->c_str()));
    ASSERT_EQ(0, fs_traversal_instance_->interface->do_link(
      context_,
      ("/" + prefix + "-foo/bar/foobar2.txt").c_str(),
      ident1->c_str()));
    ASSERT_EQ(0, fs_traversal_instance_->interface->do_link(
      context_,
      ("/" + prefix + "-foo/bar/foobar3.txt").c_str(),
      ident1->c_str()));

    // SYMLINKS
    ASSERT_EQ(0, fs_traversal_instance_->interface->do_symlink(
      context_,
      ("/" + prefix + "-symlink1").c_str(),
      "./foo",
      stat_values1));
    ASSERT_EQ(0, fs_traversal_instance_->interface->do_symlink(
      context_,
      ("/" + prefix + "-foo/bar/symlink2").c_str(),
      "./foobar",
      stat_values1));

    // Test Hash consistency checking...
    ASSERT_TRUE(fs_traversal_instance_->interface->is_hash_consistent(
      context_, stat_values1));
    ASSERT_TRUE(fs_traversal_instance_->interface->is_hash_consistent(
      context_, stat_values2));
    ASSERT_FALSE(fs_traversal_instance_->interface->is_hash_consistent(
      context_, stat_values_dir));
    ASSERT_EQ(0, fs_traversal_instance_->interface->do_link(
      context_,
      ("/" + prefix + "-foo.txt").c_str(),
      ident2->c_str()));
    ASSERT_FALSE(fs_traversal_instance_->interface->is_hash_consistent(
      context_, stat_values1));
    ASSERT_EQ(0, fs_traversal_instance_->interface->do_link(
      context_,
      ("/" + prefix + "-foo.txt").c_str(),
      ident1->c_str()));

    cvmfs_attr_free(stat_values1);
    cvmfs_attr_free(stat_values2);
    cvmfs_attr_free(stat_values_dir);
    // Check if fs is consistent over finalizing and reopening...
    Restart();
  }
 protected:
  struct fs_traversal_test *fs_traversal_instance_;
  struct fs_traversal_context *context_;
};

TEST_P(T_Fs_Traversal_Interface, StatTest) {
  std::string ident1;
  std::string ident2;
  MakeTestFiles("StatTest", &ident1, &ident2);
  struct cvmfs_attr *foostat = cvmfs_attr_init();
  struct cvmfs_attr *barstat = cvmfs_attr_init();
  struct cvmfs_attr *symlinkstat = cvmfs_attr_init();
  std::string val;
  // Correct inode configuration
  ASSERT_EQ(0, fs_traversal_instance_->interface->get_stat(
    context_, "/StatTest-foo.txt", foostat, false));
  ASSERT_STREQ("StatTest-foo.txt", foostat->cvm_name);
  ASSERT_STREQ("", foostat->cvm_parent);
  ASSERT_EQ(0770, MODE_BITMASK & foostat->st_mode);
  ASSERT_EQ(getuid(), foostat->st_uid);
  ASSERT_EQ(getgid(), foostat->st_gid);
  cvmfs_attr_free(foostat);
  foostat = cvmfs_attr_init();
  ASSERT_EQ(0, fs_traversal_instance_->interface->get_stat(
    context_, "/StatTest-foo/bar/foobar1.txt", foostat, false));
  ASSERT_STREQ("foobar1.txt", foostat->cvm_name);
  ASSERT_STREQ("/StatTest-foo/bar", foostat->cvm_parent);

  XattrList *xlist1 = reinterpret_cast<XattrList *>(foostat->cvm_xattrs);
  xlist1->Get("user.foo", &val);
  ASSERT_STREQ("StatTest", val.c_str());

  ASSERT_EQ(0, fs_traversal_instance_->interface->get_stat(
    context_, "/StatTest-bar.txt", barstat, false));
  XattrList *xlist2 = reinterpret_cast<XattrList *>(barstat->cvm_xattrs);
  xlist2->Get("user.foo", &val);
  ASSERT_STREQ("StatTest-2", val.c_str());

  ASSERT_EQ(0, fs_traversal_instance_->interface->get_stat(
    context_, "/StatTest-symlink1", symlinkstat, false));
  ASSERT_STREQ("./foo", symlinkstat->cvm_symlink);

  cvmfs_attr_free(foostat);
  cvmfs_attr_free(barstat);
  cvmfs_attr_free(symlinkstat);
}


TEST_P(T_Fs_Traversal_Interface, TouchTest) {
  std::string ident1;
  std::string ident2;
  MakeTestFiles("TouchTest", &ident1, &ident2);


  // FILE CONTENT 1
  std::string content1 = "TouchTest: Hello world!\nHello traversal!";
  shash::Any content1_hash(shash::kSha1);
  shash::HashString(content1, &content1_hash);
  std::string content2 = "TouchTest: Hello traversal!\nHello world!";
  shash::Any content2_hash(shash::kSha1);
  shash::HashString(content2, &content2_hash);
  // FILE META 3
  XattrList *xlist3 = create_sample_xattrlist("TouchTest: foo");
  struct cvmfs_attr *stat_values3 = create_sample_stat("hello.world",
    10, 0700, content1.length(), xlist3, &content1_hash);

  XattrList *xlist4 = create_sample_xattrlist("TouchTest: foobarfoo");
  struct cvmfs_attr *stat_values4 = create_sample_stat("hello.world",
    10, 0700, content1.length(), xlist4, &content1_hash);
  char *buf = fs_traversal_instance_->interface->get_identifier(
    context_,
    stat_values4);
  std::string ident4 = std::string(buf);
  free(buf);

  XattrList *xlist5 = new XattrList(*xlist4);
  struct cvmfs_attr *stat_values5 = create_sample_stat("hello.world",
    10, 0700, content1.length(), xlist5, &content2_hash);
  buf = fs_traversal_instance_->interface->get_identifier(
    context_,
    stat_values5);
  std::string ident5 = std::string(buf);
  free(buf);

  // Check directory listings
  size_t listLen = 0;
  char **dirList;
  fs_traversal_instance_->interface->list_dir(
    context_,
    "/",
    &dirList,
    &listLen);
  AssertListHas("TouchTest-foo.txt", dirList, listLen);
  AssertListHas("TouchTest-bar.txt", dirList, listLen);
  FreeList(dirList, listLen);
  listLen = 0;
  fs_traversal_instance_->interface->list_dir(
    context_,
    "/TouchTest-foo/bar/",
    &dirList,
    &listLen);
  AssertListHas("foobar1.txt", dirList, listLen);
  AssertListHas("foobar2.txt", dirList, listLen);
  AssertListHas("foobar3.txt", dirList, listLen);
  FreeList(dirList, listLen);
  listLen = 0;

  // Creating again should fail...
  ASSERT_EQ(0, fs_traversal_instance_->interface->touch(
    context_,
    stat_values3));
  ASSERT_EQ(-1, fs_traversal_instance_->interface->touch(
    context_,
    stat_values3));
  // With errno...
  ASSERT_EQ(EEXIST, errno);
  ASSERT_TRUE(fs_traversal_instance_->interface->has_file(
    context_,
    ident1.c_str()));
  ASSERT_TRUE(fs_traversal_instance_->interface->has_file(
    context_,
    ident2.c_str()));
  ASSERT_FALSE(fs_traversal_instance_->interface->has_file(
    context_,
    ident4.c_str()));
  ASSERT_FALSE(fs_traversal_instance_->interface->has_file(
    context_,
    ident5.c_str()));
  cvmfs_attr_free(stat_values3);
  cvmfs_attr_free(stat_values4);
  cvmfs_attr_free(stat_values5);
}

TEST_P(T_Fs_Traversal_Interface, MkRmDirTest) {
  std::string ident1;
  std::string ident2;
  MakeTestFiles("MkRmDirTest", &ident1, &ident2);
  XattrList *xlist1 = create_sample_xattrlist("MkRmDirTest: foo");
  struct cvmfs_attr *stat_values_dir = create_sample_stat(
      "MkRmDirTest-hello.world",
      10, 0770, 0, xlist1);
  // Insert in non existing parent directory
  ASSERT_EQ(-1, fs_traversal_instance_->interface->do_mkdir(
    context_,
    "/MkRmDirTest-foo/foobar/foobar",
    stat_values_dir));
  ASSERT_EQ(ENOENT, errno);
  // Check directory listings
  size_t listLen = 0;
  char **dirList;
  fs_traversal_instance_->interface->list_dir(
    context_,
    "/MkRmDirTest-bar",
    &dirList,
    &listLen);
  ASSERT_EQ(2, listLen);
  AssertListHas("foobar", dirList, listLen);
  AssertListHas("test", dirList, listLen);
  FreeList(dirList, listLen);
  listLen = 0;
  ASSERT_EQ(-1, fs_traversal_instance_->interface->do_rmdir(
    context_, "/MkRmDirTest-foo/foobar/foobar"));
  ASSERT_EQ(ENOENT, errno);
  // No recursive deletion
  ASSERT_EQ(-1, fs_traversal_instance_->interface->do_rmdir(
    context_, "/MkRmDirTest-bar/foobar"));
  ASSERT_EQ(0, fs_traversal_instance_->interface->do_rmdir(
    context_, "/MkRmDirTest-bar/foobar/foobar"));
  ASSERT_EQ(0, fs_traversal_instance_->interface->do_rmdir(
    context_, "/MkRmDirTest-bar/foobar"));
  fs_traversal_instance_->interface->list_dir(
    context_,
    "/MkRmDirTest-bar",
    &dirList,
    &listLen);
  ASSERT_EQ(1, listLen);
  AssertListHas("foobar", dirList, listLen, true);
  AssertListHas("test", dirList, listLen);
  FreeList(dirList, listLen);
  listLen = 0;
  ASSERT_EQ(-1, fs_traversal_instance_->interface->do_rmdir(
    context_, "/MkRmDirTest-bar/foobar"));
  ASSERT_EQ(ENOENT, errno);
  cvmfs_attr_free(stat_values_dir);
}

TEST_P(T_Fs_Traversal_Interface, ListDirTest) {
  std::string ident1;
  std::string ident2;
  MakeTestFiles("ListDirTest", &ident1, &ident2);
  size_t listLen = 0;
  char **dirList;
  fs_traversal_instance_->interface->list_dir(
    context_,
    "/",
    &dirList,
    &listLen);
  AssertListHas("ListDirTest-foo", dirList, listLen);
  AssertListHas("ListDirTest-bar", dirList, listLen);
  AssertListHas("ListDirTest-foo.txt", dirList, listLen);
  AssertListHas("ListDirTest-bar.txt", dirList, listLen);
  AssertListHas("ListDirTest-symlink1", dirList, listLen);
  FreeList(dirList, listLen);
  listLen = 0;
  fs_traversal_instance_->interface->list_dir(
    context_,
    "/ListDirTest-foo",
    &dirList,
    &listLen);
  ASSERT_EQ(1, listLen);
  AssertListHas("bar", dirList, listLen);
  FreeList(dirList, listLen);
  listLen = 0;
  fs_traversal_instance_->interface->list_dir(
    context_,
    "/ListDirTest-foo/bar",
    &dirList,
    &listLen);
  ASSERT_EQ(4, listLen);
  AssertListHas("foobar1.txt", dirList, listLen);
  AssertListHas("foobar2.txt", dirList, listLen);
  AssertListHas("foobar3.txt", dirList, listLen);
  AssertListHas("symlink2", dirList, listLen);
  FreeList(dirList, listLen);
  listLen = 0;
  fs_traversal_instance_->interface->list_dir(
    context_,
    "/ListDirTest-bar",
    &dirList,
    &listLen);
  ASSERT_EQ(2, listLen);
  AssertListHas("foobar", dirList, listLen, true);
  AssertListHas("test", dirList, listLen);
  FreeList(dirList, listLen);
  listLen = 0;
  fs_traversal_instance_->interface->list_dir(
    context_,
    "/ListDirTest-bar/foobar",
    &dirList,
    &listLen);
  AssertListHas("foobar", dirList, listLen);
  FreeList(dirList, listLen);
  listLen = 0;
}

TEST_P(T_Fs_Traversal_Interface, ReadWriteTest) {
  std::string ident1;
  std::string ident2;
  MakeTestFiles("ReadWriteTest", &ident1, &ident2);
  void *hdl1 = fs_traversal_instance_->interface->get_handle(
    context_, ident1.c_str());
  ASSERT_TRUE(NULL != hdl1);
  ASSERT_EQ(0,
    fs_traversal_instance_->interface->do_fopen(hdl1, fs_open_write));
  std::string content1 = "Lorem ipsum dolor sit amet.";
  ASSERT_EQ(0,
    fs_traversal_instance_->interface->do_fwrite(
      hdl1, content1.c_str(), content1.length()));
  char buf[50];
  size_t rlen;
  ASSERT_EQ(-1,
    fs_traversal_instance_->interface->do_fread(
      hdl1, buf, 100, &rlen));
  ASSERT_EQ(0,
    fs_traversal_instance_->interface->do_fclose(hdl1));
  ASSERT_EQ(0,
    fs_traversal_instance_->interface->do_fopen(hdl1, fs_open_read));
  ASSERT_EQ(-1,
    fs_traversal_instance_->interface->do_fwrite(
      hdl1, content1.c_str(), content1.length()));
  ASSERT_EQ(0,
    fs_traversal_instance_->interface->do_fread(
      hdl1, buf, 100, &rlen));
  buf[rlen] = '\0';
  ASSERT_EQ(content1.length(), rlen);
  ASSERT_STREQ(content1.c_str(), buf);
  ASSERT_EQ(0,
    fs_traversal_instance_->interface->do_fclose(hdl1));
  fs_traversal_instance_->interface->do_ffree(hdl1);
}

TEST_P(T_Fs_Traversal_Interface, LinkUnlinkTest) {
  std::string ident1;
  std::string ident2;
  MakeTestFiles("LinkUnlinkTest", &ident1, &ident2);
  struct cvmfs_attr *foostat = cvmfs_attr_init();
  struct cvmfs_attr *barstat = cvmfs_attr_init();
  struct cvmfs_attr *foobarstat = cvmfs_attr_init();
  // Correct inode configuration
  ASSERT_EQ(0, fs_traversal_instance_->interface->get_stat(
    context_, "/LinkUnlinkTest-foo.txt", foostat, false));
  ASSERT_EQ(0, fs_traversal_instance_->interface->get_stat(
    context_, "/LinkUnlinkTest-bar.txt", barstat, false));
  ASSERT_NE(foostat->st_ino, barstat->st_ino);
  ASSERT_EQ(0, fs_traversal_instance_->interface->get_stat(
    context_, "/LinkUnlinkTest-foo/bar/foobar1.txt", foobarstat, false));
  ASSERT_EQ(foostat->st_ino, foobarstat->st_ino);
  cvmfs_attr_free(foobarstat);
  foobarstat = cvmfs_attr_init();
  ASSERT_EQ(0, fs_traversal_instance_->interface->get_stat(
    context_, "/LinkUnlinkTest-foo/bar/foobar2.txt", foobarstat, false));
  ASSERT_EQ(foostat->st_ino, foobarstat->st_ino);
  cvmfs_attr_free(foobarstat);
  foobarstat = cvmfs_attr_init();
  ASSERT_EQ(0, fs_traversal_instance_->interface->get_stat(
    context_, "/LinkUnlinkTest-foo/bar/foobar3.txt", foobarstat, false));
  ASSERT_EQ(foostat->st_ino, foobarstat->st_ino);
  cvmfs_attr_free(foobarstat);
  foobarstat = cvmfs_attr_init();
  ASSERT_EQ(-1, fs_traversal_instance_->interface->do_link(
    context_, "/LinkUnlinkTest-foobar/foofoobar/foofoofoobar", ident1.c_str()));
  ASSERT_EQ(ENOENT, errno);
  ASSERT_EQ(-1, fs_traversal_instance_->interface->do_link(
    context_, "/LinkUnlinkTest-foobar.txt", "foobarfoobar"));
  ASSERT_EQ(ENOENT, errno);
  ASSERT_EQ(0, fs_traversal_instance_->interface->do_link(
    context_, "/LinkUnlinkTest-foo/bar/foobar3.txt", ident2.c_str()));
  ASSERT_EQ(0, fs_traversal_instance_->interface->get_stat(
    context_, "/LinkUnlinkTest-foo/bar/foobar3.txt", foobarstat, false));
  ASSERT_EQ(barstat->st_ino, foobarstat->st_ino);

  cvmfs_attr_free(foostat);
  cvmfs_attr_free(barstat);
  cvmfs_attr_free(foobarstat);

  // Correct unlink behaviour
  ASSERT_EQ(-1, fs_traversal_instance_->interface->do_unlink(
    context_, "/LinkUnlinkTest-foobar/foofoobar/foofoofoobar"));
  ASSERT_EQ(ENOENT, errno);
  ASSERT_EQ(-1, fs_traversal_instance_->interface->do_unlink(
    context_, "/LinkUnlinkTest-bar/foobar/foobar"));
  ASSERT_EQ(EISDIR, errno);
}

TEST_P(T_Fs_Traversal_Interface, SymlinkTest) {
  std::string ident1;
  std::string ident2;
  MakeTestFiles("SymlinkTest", &ident1, &ident2);
  struct cvmfs_attr *sl1Stat = cvmfs_attr_init();
  struct cvmfs_attr *sl2Stat = cvmfs_attr_init();
  ASSERT_EQ(0, fs_traversal_instance_->interface->get_stat(
    context_, "/SymlinkTest-symlink1", sl1Stat, false));
  ASSERT_STREQ("./foo", sl1Stat->cvm_symlink);
  ASSERT_EQ(0, fs_traversal_instance_->interface->get_stat(
    context_, "/SymlinkTest-foo/bar/symlink2", sl2Stat, false));
  ASSERT_STREQ("./foobar", sl2Stat->cvm_symlink);
  cvmfs_attr_free(sl1Stat);
  cvmfs_attr_free(sl2Stat);
}

TEST_P(T_Fs_Traversal_Interface, TransferPosixToPosix) {
  std::string ident1;
  std::string ident2;
  std::string prefix = "SRC";
  MakeTestFiles(prefix, &ident1, &ident2);

  std::string repoName = GetCurrentWorkingDirectory();
  std::string src_name  = "/SRC-foo";
  std::string dest_name = "/SRC-bar";
  std::string dest_data = repoName+"/SRC-DDATA";

  struct fs_traversal *src = posix_get_interface();
  struct fs_traversal_context *context =
    src->initialize(
      src_name.c_str(), repoName.c_str(), NULL, NULL, 4);
  src->context_ = context;

  struct fs_traversal *dest = posix_get_interface();
  context = dest->initialize(
    src_name.c_str(), repoName.c_str(), NULL, NULL, 4);
  dest->context_ = context;

  perf::Statistics *statistics = shrinkwrap::GetSyncStatTemplate();

  ASSERT_TRUE(shrinkwrap::SyncFull(src, dest, statistics));

  dest->finalize(dest->context_);
  context = dest->initialize(
    dest_name.c_str(), repoName.c_str(), dest_data.c_str(), NULL, 4);
  dest->context_ = context;

  ASSERT_TRUE(shrinkwrap::SyncFull(src, dest, statistics));
  std::string command = "diff -aq ";
  command += repoName + "/" + dest_name;
  command += " " + repoName + "/" + src_name;
  cerr << command.c_str() << endl;
  ASSERT_EQ(0, system(command.c_str()));

  src->finalize(src->context_);
  dest->finalize(dest->context_);
  delete statistics;
  delete src;
  delete dest;
}

struct fs_traversal_test posix = {
  posix_get_interface(),
  "./",
  "./.data"
};

INSTANTIATE_TEST_CASE_P(PosixInterfaceTest,
                        T_Fs_Traversal_Interface,
                        ::testing::Values(&posix));
