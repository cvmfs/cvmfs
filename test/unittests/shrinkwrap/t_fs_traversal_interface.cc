/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <ctime>
#include <iostream>

#include "catalog_test_tools.h"
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
    char * val = strdup(content1.c_str());
    fwrite(val, sizeof(char), content1.length(), f);
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
    *ident1 = std::string(
      fs_traversal_instance_->interface->get_identifier(context_,
      stat_values1));
    *ident2 = std::string(
      fs_traversal_instance_->interface->get_identifier(context_,
        stat_values2));

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
  std::string ident4 = fs_traversal_instance_->interface->get_identifier(
    context_,
    stat_values4);

  XattrList *xlist5 = new XattrList(*xlist4);
  struct cvmfs_attr *stat_values5 = create_sample_stat("hello.world",
    10, 0700, content1.length(), xlist5, &content2_hash);
  std::string ident5 = fs_traversal_instance_->interface->get_identifier(
    context_,
    stat_values5);

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
  listLen = 0;
  delete dirList;
  fs_traversal_instance_->interface->list_dir(
    context_,
    "/TouchTest-foo/bar/",
    &dirList,
    &listLen);
  AssertListHas("foobar1.txt", dirList, listLen);
  AssertListHas("foobar2.txt", dirList, listLen);
  AssertListHas("foobar3.txt", dirList, listLen);
  listLen = 0;
  delete dirList;

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
  listLen = 0;
  delete dirList;
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
  listLen = 0;
  delete dirList;
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
  listLen = 0;
  delete dirList;
  fs_traversal_instance_->interface->list_dir(
    context_,
    "/ListDirTest-foo",
    &dirList,
    &listLen);
  ASSERT_EQ(1, listLen);
  AssertListHas("bar", dirList, listLen);
  listLen = 0;
  delete dirList;
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
  listLen = 0;
  delete dirList;
  fs_traversal_instance_->interface->list_dir(
    context_,
    "/ListDirTest-bar",
    &dirList,
    &listLen);
  ASSERT_EQ(2, listLen);
  AssertListHas("foobar", dirList, listLen, true);
  AssertListHas("test", dirList, listLen);
  listLen = 0;
  delete dirList;
  fs_traversal_instance_->interface->list_dir(
    context_,
    "/ListDirTest-bar/foobar",
    &dirList,
    &listLen);
  AssertListHas("foobar", dirList, listLen);
  listLen = 0;
  delete dirList;
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
  ASSERT_EQ(0, fs_traversal_instance_->interface->get_stat(
    context_, "/LinkUnlinkTest-foo/bar/foobar2.txt", foobarstat, false));
  ASSERT_EQ(foostat->st_ino, foobarstat->st_ino);
  ASSERT_EQ(0, fs_traversal_instance_->interface->get_stat(
    context_, "/LinkUnlinkTest-foo/bar/foobar3.txt", foobarstat, false));
  ASSERT_EQ(foostat->st_ino, foobarstat->st_ino);
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

  EXPECT_TRUE(shrinkwrap::SyncFull(src, dest, statistics));

  src->finalize(src->context_);
  dest->finalize(dest->context_);
}

// Create some default hashes for DirSpec
const char* hashs[] = {"b026324c6904b2a9cb4b88d6d61c81d1000000",
                        "26ab0db90d72e28ad0ba1e22ee510510000000",
                        "6d7fce9fee471194aa8b5b6e47267f03000000",
                        "48a24b70a0b376535542b996af517398000000",
                        "1dcca23355272056f04fe8bf20edfce0000000",
                        "11111111111111111111111111111111111111"};

const size_t file_size = 4096;

// Create directory specification for later repositories
DirSpec MakeSpec() {
  DirSpec spec;

  // adding "/dir"
  EXPECT_TRUE(spec.AddDirectory("dir", "", file_size));

  // adding "/dir/file1"
  EXPECT_TRUE(spec.AddFile("file1", "dir", hashs[0], file_size));

  // adding "/dir/dir"
  EXPECT_TRUE(spec.AddDirectory("dir",  "dir", file_size));
  EXPECT_TRUE(spec.AddDirectory("dir2", "dir", file_size));
  EXPECT_TRUE(spec.AddDirectory("dir3", "dir", file_size));

  // adding "/file3"
  EXPECT_TRUE(spec.AddFile("file3", "", hashs[2], file_size));

  // adding "/dir/dir/file2"
  EXPECT_TRUE(spec.AddFile("file2", "dir/dir", hashs[1], file_size));

  // adding "/dir/dir2/file2"
  EXPECT_TRUE(spec.AddFile("file2", "dir/dir2", hashs[3], file_size));

  // adding "/dir/dir3/file2"
  EXPECT_TRUE(spec.AddFile("file2", "dir/dir3", hashs[4], file_size));

  // Adding Deeply nested catalog
  EXPECT_TRUE(spec.AddDirectory("dir",  "dir/dir", file_size));
  EXPECT_TRUE(spec.AddDirectory("dir",  "dir/dir/dir", file_size));
  EXPECT_TRUE(spec.AddFile("file1",  "dir/dir/dir/dir", hashs[0], file_size));

  return spec;
}

<<<<<<< HEAD
TEST(T_Fs_Traversal_POSIX, TestGarbageCollection) {
  struct fs_traversal *dest = posix_get_interface();
  struct fs_traversal_context *context
    = dest->initialize("./", "posix", "./data", 4, NULL);

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
  context = dest->initialize("./", "posix", "./data", 4, NULL);
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

=======
TEST(T_Fs_Traversal_CVMFS, TransferCVMFSToPosix) {
  // Initialize options
  cvmfs_option_map *opts = cvmfs_options_init();

  // Create and initialize repository named "stat"
  CatalogTestTool tester("TransferCVMFSToPosix");
  EXPECT_TRUE(tester.Init());

  // Create file structure
  DirSpec spec1 = MakeSpec();
  EXPECT_TRUE(tester.ApplyAtRootHash(tester.manifest()->catalog_hash(), spec1));

  // Find directory entry for use later
  catalog::DirectoryEntry entry;
  EXPECT_TRUE(
    tester.FindEntry(tester.manifest()->catalog_hash(), "/dir/file1", &entry));

  // Set CVMFS options to reflect created repository
  cvmfs_options_set(opts, "CVMFS_ROOT_HASH",
                        tester.manifest()->catalog_hash().ToString().c_str());
  cvmfs_options_set(opts, "CVMFS_SERVER_URL",
                        ("file://" + tester.repo_name()).c_str());
  cvmfs_options_set(opts, "CVMFS_HTTP_PROXY", "DIRECT");
  cvmfs_options_set(opts, "CVMFS_PUBLIC_KEY",
                        tester.public_key().c_str());
  cvmfs_options_set(opts, "CVMFS_CACHE_DIR",
                        (tester.repo_name()+"/data/txn").c_str());
  cvmfs_options_set(opts, "CVMFS_MOUNT_DIR",
                        ("/cvmfs" + tester.repo_name()).c_str());

  // Initialize client repo based on options
  ASSERT_EQ(LIBCVMFS_ERR_OK, cvmfs_init_v2(opts));

  // Attach to client repo
  cvmfs_context *ctx;
  EXPECT_EQ(LIBCVMFS_ERR_OK,
    cvmfs_attach_repo_v2((tester.repo_name().c_str()), opts, &ctx));

  struct fs_traversal *src = libcvmfs_get_interface();
  struct fs_traversal_context *context;
  context = src->initialize(tester.repo_name().c_str(), NULL, NULL, 4, NULL);
  context->ctx = ctx;
  src->context_ = context;

  mkdir("posix", 0770);
  struct fs_traversal *dest = posix_get_interface();
  context = dest->initialize("./", "posix", "posix_data", 4, NULL);
  dest->context_ = context;

  perf::Statistics *statistics = shrinkwrap::GetSyncStatTemplate();

  EXPECT_TRUE(shrinkwrap::Sync("", src, dest, true, statistics, false));

  src->finalize(src->context_);
  dest->finalize(dest->context_);

  // Finalize and close repo and options
  cvmfs_options_fini(opts);
}
>>>>>>> Added fsck for synchronization as well as a (still simple) unit test. Furthermore a few test

struct fs_traversal_test posix = {
  posix_get_interface(),
  "./",
  ".data"
};

INSTANTIATE_TEST_CASE_P(PosixInterfaceTest,
                        T_Fs_Traversal_Interface,
                        ::testing::Values(&posix));
