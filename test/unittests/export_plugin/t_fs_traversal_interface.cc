/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <errno.h>
#include <string.h>
#include <unistd.h>

#include <ctime>

#include "libcvmfs.h"
#include "util/posix.h"
#include "xattr.h"

#include "export_plugin/fs_traversal_interface.h"
#include "export_plugin/fs_traversal_posix.h"
#include "export_plugin/util.h"

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
      std::string repoName = GetCurrentWorkingDirectory();
      repo = strdup(repoName.c_str());
    }
    context_ = fs_traversal_instance_->interface->initialize(
      repo,
      fs_traversal_instance_->data);
  }

  void Fin() {
    fs_traversal_instance_->interface->finalize(context_);
    context_ = NULL;
  }

  XattrList *create_sample_xattrlist(std::string var) {
    XattrList *result = new XattrList();
    result->Set("foo", var);
    result->Set("bar", std::string(256, 'a'));
    return result;
  }
  struct cvmfs_stat create_sample_stat(const char *name,
    ino_t st_ino, mode_t st_mode, off_t st_size,
    XattrList *xlist, shash::Any *content_hash = NULL,
    const char *link = NULL) {
    const char *hash_result = NULL;
    if (content_hash != NULL) {
      std::string hash = content_hash->ToString();
      hash_result = strdup(hash.c_str());
    }
    const char *link_result = NULL;
    if (link != NULL) {
      link_result = strdup(link);
    }
    struct cvmfs_stat result = {
      1,
      sizeof(struct cvmfs_stat),

      st_ino,         // st_ino
      st_mode,        // st_mode
      1,              // st_nlink
      getuid(),       // st_uid
      getgid(),       // st_gid
      0,              // st_rdev
      st_size,
      1024,
      st_size / 1024,
      time(NULL),

      hash_result,
      link_result,
      strdup(name),
      xlist
    };
    return result;
  }
  void free_stat_attr(const struct cvmfs_stat *stat) {
    if (stat->cvm_checksum != NULL) {
      delete stat->cvm_checksum;
    }
    if (stat->cvm_symlink != NULL) {
      delete stat->cvm_symlink;
    }
    if (stat->cvm_name != NULL) {
      delete stat->cvm_name;
    }
    if (stat->cvm_xattrs != NULL) {
      delete reinterpret_cast<XattrList *>(stat->cvm_xattrs);
    }
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
    // FILE CONTENT 2
    std::string content2 = prefix + ": Hello traversal!\nHello world!";
    shash::Any content2_hash(shash::kSha1);
    shash::HashString(content2, &content2_hash);
    // FILE META 1
    XattrList *xlistdir = create_sample_xattrlist(prefix);
    const struct cvmfs_stat stat_values_dir = create_sample_stat(
      (prefix + "-hello.world").c_str(),
      10, 0770, 0, xlistdir);
    XattrList *xlist1 = create_sample_xattrlist(prefix);
    const struct cvmfs_stat stat_values1 = create_sample_stat(
      (prefix + "-hello.world").c_str(),
      10, 0770, 0, xlist1, &content1_hash);
    XattrList *xlist2 = create_sample_xattrlist(prefix);
    const struct cvmfs_stat stat_values2 = create_sample_stat(
      (prefix + "-hello.world").c_str(),
      10, 0770, 0, xlist2, &content2_hash);
    *ident1 = std::string(
      fs_traversal_instance_->interface->get_identifier(context_,
      &stat_values1));
    *ident2 = std::string(
      fs_traversal_instance_->interface->get_identifier(context_,
      &stat_values2));
    // BACKGROUND FILES
    ASSERT_EQ(0, fs_traversal_instance_->interface->touch(
      context_,
      &stat_values1));
    ASSERT_EQ(0, fs_traversal_instance_->interface->touch(
      context_,
      &stat_values2));
    // DIRECTORIES
    ASSERT_EQ(0, fs_traversal_instance_->interface->do_mkdir(
      context_,
      ("/" + prefix + "-foo").c_str(),
      &stat_values_dir));
    ASSERT_EQ(0, fs_traversal_instance_->interface->do_mkdir(
      context_,
      ("/" + prefix + "-bar").c_str(),
      &stat_values_dir));
    ASSERT_EQ(0, fs_traversal_instance_->interface->do_mkdir(
      context_,
      ("/" + prefix + "-bar/test").c_str(),
      &stat_values_dir));
    ASSERT_EQ(0, fs_traversal_instance_->interface->do_mkdir(
      context_,
      ("/" + prefix + "-foo/bar").c_str(),
      &stat_values_dir));
    ASSERT_EQ(0, fs_traversal_instance_->interface->do_mkdir(
      context_,
      ("/" + prefix + "-bar/foobar").c_str(),
      &stat_values_dir));
    ASSERT_EQ(0, fs_traversal_instance_->interface->do_mkdir(
      context_,
      ("/" + prefix + "-bar/foobar/foobar").c_str(),
      &stat_values_dir));

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
      &stat_values1));
    ASSERT_EQ(0, fs_traversal_instance_->interface->do_symlink(
      context_,
      ("/" + prefix + "-foo/bar/symlink2").c_str(),
      "./foobar",
      &stat_values1));
    free_stat_attr(&stat_values1);
    free_stat_attr(&stat_values2);
    free_stat_attr(&stat_values_dir);
  }
  void AssertListHas(const char *query, char **dirList, size_t listLen,
    bool hasNot = false) {
    char **curEl = dirList;
    for (size_t i = 0; i < listLen; i++) {
      if (strcmp(*curEl, query) == 0) {
        return;
      }
      curEl = (curEl+1);
    }
    ASSERT_TRUE(hasNot) << "Could not find element " << query << " in list";
  }
 protected:
  struct fs_traversal_test *fs_traversal_instance_;
  struct fs_traversal_context *context_;
};

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
  struct cvmfs_stat stat_values3 = create_sample_stat("hello.world",
    10, 0700, content1.length(), xlist3, &content1_hash);

  XattrList *xlist4 = create_sample_xattrlist("TouchTest: foobarfoo");
  struct cvmfs_stat stat_values4 = create_sample_stat("hello.world",
    10, 0700, content1.length(), xlist4, &content1_hash);
  std::string ident4 = fs_traversal_instance_->interface->get_identifier(
    context_,
    &stat_values4);

  XattrList *xlist5 = new XattrList(*xlist4);
  struct cvmfs_stat stat_values5 = create_sample_stat("hello.world",
    10, 0700, content1.length(), xlist5, &content2_hash);
  std::string ident5 = fs_traversal_instance_->interface->get_identifier(
    context_,
    &stat_values5);

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
    &stat_values3));
  ASSERT_EQ(-1, fs_traversal_instance_->interface->touch(
    context_,
    &stat_values3));
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
  free_stat_attr(&stat_values3);
  free_stat_attr(&stat_values4);
  free_stat_attr(&stat_values5);
}

TEST_P(T_Fs_Traversal_Interface, MkRmDirTest) {
  std::string ident1;
  std::string ident2;
  MakeTestFiles("MkRmDirTest", &ident1, &ident2);
  XattrList *xlist1 = create_sample_xattrlist("MkRmDirTest: foo");
  struct cvmfs_stat stat_values_dir = create_sample_stat(
      "MkRmDirTest-hello.world",
      10, 0770, 0, xlist1);
  // Insert in non existing parent directory
  ASSERT_EQ(-1, fs_traversal_instance_->interface->do_mkdir(
    context_,
    "/MkRmDirTest-foo/foobar/foobar",
    &stat_values_dir));
  ASSERT_EQ(ENOENT, errno);
  // Check directory listings
  size_t listLen = 0;
  char **dirList;
  fs_traversal_instance_->interface->list_dir(
    context_,
    "/",
    &dirList,
    &listLen);
  AssertListHas("MkRmDirTest-foo", dirList, listLen);
  AssertListHas("MkRmDirTest-bar", dirList, listLen);
  listLen = 0;
  delete dirList;
  fs_traversal_instance_->interface->list_dir(
    context_,
    "/MkRmDirTest-foo",
    &dirList,
    &listLen);
  ASSERT_EQ(1, listLen);
  AssertListHas("bar", dirList, listLen);
  listLen = 0;
  delete dirList;
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
  fs_traversal_instance_->interface->list_dir(
    context_,
    "/MkRmDirTest-bar/foobar",
    &dirList,
    &listLen);
  AssertListHas("foobar", dirList, listLen);
  listLen = 0;
  delete dirList;
  ASSERT_EQ(-1, fs_traversal_instance_->interface->do_rmdir(
    context_, "/MkRmDirTest-foo/foobar/foobar"));
  ASSERT_EQ(ENOENT, errno);
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
  free_stat_attr(&stat_values_dir);
}

TEST_P(T_Fs_Traversal_Interface, SymlinkTest) {
  std::string ident1;
  std::string ident2;
  MakeTestFiles("SymlinkTest", &ident1, &ident2);
  struct cvmfs_stat sl1Stat;
  struct cvmfs_stat sl2Stat;
  ASSERT_EQ(0, fs_traversal_instance_->interface->get_stat(
    context_, "/SymlinkTest-symlink1", &sl1Stat));
  ASSERT_STREQ("./foo", sl1Stat.cvm_symlink);
  ASSERT_EQ(0, fs_traversal_instance_->interface->get_stat(
    context_, "/SymlinkTest-foo/bar/symlink2", &sl2Stat));
  ASSERT_STREQ("./foobar", sl2Stat.cvm_symlink);
}


struct fs_traversal_test posix = {
  posix_get_interface(),
  NULL,
  ".data"
};

INSTANTIATE_TEST_CASE_P(PosixInterfaceTest,
                        T_Fs_Traversal_Interface,
                        ::testing::Values(&posix));
