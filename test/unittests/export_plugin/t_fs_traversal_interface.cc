/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <errno.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

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
    //delete fs_traversal_instance_->interface;
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
  /**
   * Creates the following entries:
   * DIRECTORIES:
   * /[prefix]-foo
   * /[prefix]-foo/bar
   * /[prefix]-foo/foobar
   * /[prefix]-foo/foobar/foobar
   * /[prefix]-bar
   * /[prefix]-bar/test
   * 
   * SYMLINKS:
   * /[prefix]-symlink1
   * /[prefix]-foo/bar/symlink2
   */
  void MakeTestFiles(std::string prefix) {
    // FILE CONTENT 1
  std::string content1 = prefix + ": Hello world!\nHello traversal!";
  shash::Any content1_hash(shash::kSha1);
  shash::HashString(content1, &content1_hash);
  // FILE CONTENT 2
  std::string content2 = prefix + ": Hello traversal!\nHello world!";
  shash::Any content2_hash(shash::kSha1);
  shash::HashString(content2, &content2_hash);
  // FILE META 1
  XattrList *xlist1 = create_sample_xattrlist("foo");
  struct cvmfs_stat stat_values1 = create_sample_stat(prefix + "-hello.world",
    10, 0770, 0, xlist1);
  shash::Any meta1_hash = HashMeta(&stat_values1);
  // BACKGROUND FILES
  ASSERT_EQ(0, fs_traversal_instance_->interface->touch(
    context_,
    &content1_hash,
    &meta1_hash,
    &stat_values1));
  ASSERT_EQ(0, fs_traversal_instance_->interface->touch(
    context_,
    &content2_hash,
    &meta1_hash,
    &stat_values1));
  // DIRECTORIES
  ASSERT_EQ(0, fs_traversal_instance_->interface->do_mkdir(
    context_,
    ("/"+prefix+"-foo").c_str(),
    &stat_values1));
  ASSERT_EQ(0, fs_traversal_instance_->interface->do_mkdir(
    context_,
    ("/"+prefix+"-bar").c_str(),
    &stat_values1));
  ASSERT_EQ(0, fs_traversal_instance_->interface->do_mkdir(
    context_,
    ("/"+prefix+"-bar/test").c_str(),
    &stat_values1));
  ASSERT_EQ(0, fs_traversal_instance_->interface->do_mkdir(
    context_,
    ("/"+prefix+"-foo/bar").c_str(),
    &stat_values1));
  ASSERT_EQ(0, fs_traversal_instance_->interface->do_mkdir(
    context_,
    ("/"+prefix+"-bar/foobar").c_str(),
    &stat_values1));
  ASSERT_EQ(0, fs_traversal_instance_->interface->do_mkdir(
    context_,
    ("/"+prefix+"-bar/foobar/foobar").c_str(),
    &stat_values1));

  // FILES
  

  // SYMLINKS
  ASSERT_EQ(0, fs_traversal_instance_->interface->do_symlink(
    context_,
    ("/"+prefix+"-symlink1").c_str(),
    "/foo",
    &stat_values1));
  ASSERT_EQ(0, fs_traversal_instance_->interface->do_symlink(
    context_,
    ("/"+prefix+"-foo/bar/symlink2").c_str(),
    "/foo",
    &stat_values1));
  }

  struct cvmfs_stat create_sample_stat(const char *name,
    ino_t st_ino, mode_t st_mode, off_t st_size, XattrList *xlist) {
    struct timespec spec;
    clock_gettime(CLOCK_REALTIME, &spec);

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
      spec,

      NULL,
      NULL,
      name,
      xlist
    };
    return result;
  }
 protected:
  struct fs_traversal_test *fs_traversal_instance_;
  struct fs_traversal_context *context_;
};

TEST_P(T_Fs_Traversal_Interface, TouchTest) {
  // FILE CONTENT 1
  std::string content1 = "TouchTest: Hello world!\nHello traversal!";
  shash::Any content1_hash(shash::kSha1);
  shash::HashString(content1, &content1_hash);
  // FILE CONTENT 2
  std::string content2 = "TouchTest: Hello traversal!\nHello world!";
  shash::Any content2_hash(shash::kSha1);
  shash::HashString(content2, &content2_hash);
  // FILE META 1
  XattrList *xlist1 = create_sample_xattrlist("foo");
  struct cvmfs_stat stat_values1 = create_sample_stat("hello.world",
    10, 0770, content1.length(), xlist1);
  shash::Any meta1_hash = HashMeta(&stat_values1);
  // FILE META 2
  XattrList *xlist2 = create_sample_xattrlist("bar");
  struct cvmfs_stat stat_values2 = create_sample_stat("hello.world",
    10, 0770, content1.length(), xlist2);
  shash::Any meta2_hash = HashMeta(&stat_values2);
  // FILE META 3
  XattrList *xlist3 = create_sample_xattrlist("foo");
  struct cvmfs_stat stat_values3 = create_sample_stat("hello.world",
    10, 0700, content1.length(), xlist3);
  shash::Any meta3_hash = HashMeta(&stat_values3);

  ASSERT_EQ(0, fs_traversal_instance_->interface->touch(
    context_,
    &content1_hash,
    &meta1_hash,
    &stat_values1));
  ASSERT_EQ(0, fs_traversal_instance_->interface->touch(
    context_,
    &content1_hash,
    &meta2_hash,
    &stat_values2));
  ASSERT_EQ(0, fs_traversal_instance_->interface->touch(
    context_,
    &content1_hash,
    &meta3_hash,
    &stat_values3));
  ASSERT_EQ(0, fs_traversal_instance_->interface->touch(
    context_,
    &content2_hash,
    &meta1_hash,
    &stat_values1));
  // Creating again should fail...
  ASSERT_EQ(-1, fs_traversal_instance_->interface->touch(
    context_,
    &content2_hash,
    &meta1_hash,
    &stat_values1));
  // With errno...
  ASSERT_EQ(EEXIST, errno);
  ASSERT_TRUE(fs_traversal_instance_->interface->has_hash(
    context_,
    &content1_hash,
    &meta1_hash));
  ASSERT_TRUE(fs_traversal_instance_->interface->has_hash(
    context_,
    &content1_hash,
    &meta2_hash));
  ASSERT_TRUE(fs_traversal_instance_->interface->has_hash(
    context_,
    &content1_hash,
    &meta3_hash));
  ASSERT_TRUE(fs_traversal_instance_->interface->has_hash(
    context_,
    &content2_hash,
    &meta1_hash));
  ASSERT_FALSE(fs_traversal_instance_->interface->has_hash(
    context_,
    &content2_hash,
    &meta2_hash));
}

TEST_P(T_Fs_Traversal_Interface, RmMkDir) {
  XattrList *xlist1 = create_sample_xattrlist("foo");
  struct cvmfs_stat stat_values1 = create_sample_stat("hello.world",
    10, 0770, 0, xlist1);
  ASSERT_EQ(-1, fs_traversal_instance_->interface->do_mkdir(
    context_,
    "/RmMkDir-foo/foo/foo",
    &stat_values1));
  ASSERT_EQ(ENOENT, errno);
  ASSERT_EQ(0, fs_traversal_instance_->interface->do_mkdir(
    context_,
    "/RmMkDir-foo/foo",
    &stat_values1));

  ASSERT_EQ(0, fs_traversal_instance_->interface->do_rmdir(
    context_,
    "/RmMkDir-foo/bar"));
  ASSERT_EQ(0, fs_traversal_instance_->interface->do_rmdir(
    context_,
    "/RmMkDir-foo"));
  ASSERT_EQ(-1, fs_traversal_instance_->interface->do_rmdir(
    context_,
    "/RmMkDir-bar/test"));
  ASSERT_EQ(ENOENT, errno);
  ASSERT_EQ(0, fs_traversal_instance_->interface->do_rmdir(
    context_,
    "/RmMkDir-bar"));
  ASSERT_EQ(-1, fs_traversal_instance_->interface->do_rmdir(
    context_,
    "/RmMkDir-bar"));
  ASSERT_EQ(ENOENT, errno);
}

TEST_P(T_Fs_Traversal_Interface, CheckLink) {
  // FILE CONTENT 1
  std::string content1 = "CheckLink: Hello world!\nHello traversal!";
  shash::Any content1_hash(shash::kSha1);
  shash::HashString(content1, &content1_hash);
  // FILE CONTENT 2
  std::string content2 = "CheckLink: Hello traversal!\nHello world!";
  shash::Any content2_hash(shash::kSha1);
  shash::HashString(content2, &content2_hash);
  // FILE HASH 3
  std::string content3 = "CheckLink: I won't exist";
  shash::Any content3_hash(shash::kSha1);
  shash::HashString(content3, &content3_hash);
  // FILE META 1
  XattrList *xlist1 = create_sample_xattrlist("foo");
  struct cvmfs_stat stat_values1 = create_sample_stat("hello.world",
    10, 0770, 0, xlist1);
  shash::Any meta1_hash = HashMeta(&stat_values1);
  // BACKGROUND FILES
  ASSERT_EQ(0, fs_traversal_instance_->interface->touch(
    context_,
    &content1_hash,
    &meta1_hash,
    &stat_values1));
  ASSERT_EQ(0, fs_traversal_instance_->interface->touch(
    context_,
    &content2_hash,
    &meta1_hash,
    &stat_values1));
  // DIRECTORY
  ASSERT_EQ(0, fs_traversal_instance_->interface->do_mkdir(
    context_,
    "/CheckLink-foo",
    &stat_values1));
  ASSERT_EQ(0, fs_traversal_instance_->interface->do_mkdir(
    context_,
    "/CheckLink-foobar",
    &stat_values1));
  // Overwritable Symlink
  // TODO(steuber): 
  ASSERT_EQ(0, fs_traversal_instance_->interface->do_symlink(
    context_,
    "/CheckLink-overwritesymlink",
    "/foo",
    &stat_values1));
  // Make links
  ASSERT_EQ(0, fs_traversal_instance_->interface->do_link(
    context_,
    "/CheckLink-bar.txt",
    &content1_hash,
    &meta1_hash));
  // Rewrite link
  ASSERT_EQ(0, fs_traversal_instance_->interface->do_link(
    context_,
    "/CheckLink-bar.txt",
    &content2_hash,
    &meta1_hash));
  // Make link in subdir
  ASSERT_EQ(0, fs_traversal_instance_->interface->do_link(
    context_,
    "/CheckLink-foo/CheckLink-bar.txt",
    &content2_hash,
    &meta1_hash));
  // Write link with not existing inode
  ASSERT_EQ(-1, fs_traversal_instance_->interface->do_link(
    context_,
    "/CheckLink-foo/CheckLink-foo.txt",
    &content3_hash,
    &meta1_hash));
  ASSERT_EQ(ENOENT, errno);
  // Try to overwrite link for filled dir
  ASSERT_EQ(0, fs_traversal_instance_->interface->do_link(
    context_,
    "/CheckLink-foo",
    &content2_hash,
    &meta1_hash));
  // Try to overwrite link for empty dir
  ASSERT_EQ(0, fs_traversal_instance_->interface->do_link(
    context_,
    "/CheckLink-foobar",
    &content2_hash,
    &meta1_hash));
  // Try to overwrite symlink:
  ASSERT_EQ(0, fs_traversal_instance_->interface->do_link(
    context_,
    "/CheckLink-overwritesymlink",
    &content2_hash,
    &meta1_hash));
  ASSERT_EQ(-1, fs_traversal_instance_->interface->do_link(
    context_,
    "/CheckLink-foobar/CheckLink-bar.txt",
    &content1_hash,
    &meta1_hash));
  ASSERT_EQ(ENOENT, errno);
  ASSERT_EQ(-1, fs_traversal_instance_->interface->do_link(
    context_,
    "/CheckLink-bar/CheckLink-bar.txt",
    &content1_hash,
    &meta1_hash));
  ASSERT_EQ(ENOENT, errno);
}



struct fs_traversal_test posix = {
  posix_get_interface(),
  NULL,
  ".data"
};

INSTANTIATE_TEST_CASE_P(PosixInterfaceTest,
                        T_Fs_Traversal_Interface,
                        ::testing::Values(&posix));
