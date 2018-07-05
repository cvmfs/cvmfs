/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <errno.h>

#include <time.h>

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
    fs_traversal_instance_ = GetParam();
    context_ = fs_traversal_instance_->interface->initialize(
      fs_traversal_instance_->repo,
      fs_traversal_instance_->data);
  }

  virtual void TearDown() {
    // TODO(steuber): Can we generalize this?
    fs_traversal_instance_->interface->finalize(context_);
    context_ = NULL;
    delete fs_traversal_instance_->interface;
  }

  XattrList *create_sample_xattrlist(std::string var) {
    XattrList *result = new XattrList();
    result->Set("foo", var);
    result->Set("bar", std::string(256, 'a'));
    return result;
  }
  struct cvmfs_stat create_sample_stat(const char *name,
    ino_t st_ino, mode_t st_mode, uid_t st_uid, gid_t st_gid,
    off_t st_size, XattrList *xlist) {
    struct timespec spec;
    clock_gettime(CLOCK_REALTIME, &spec);

    struct cvmfs_stat result = {
      1,
      sizeof(struct cvmfs_stat),

      st_ino,  // st_ino
      st_mode,  // st_mode
      st_uid,  // st_nlink
      st_gid,  // st_uid
      st_gid,  // st_gid
      0,  // st_rdev
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
  std::string content1 = "Hello world!\nHello traversal!";
  shash::Any content1_hash(shash::kSha1);
  shash::HashString(content1, &content1_hash);
  // FILE CONTENT 2
  std::string content2 = "Hello traversal!\nHello world!";
  shash::Any content2_hash(shash::kSha1);
  shash::HashString(content2, &content2_hash);
  // FILE META 1
  XattrList *xlist1 = create_sample_xattrlist("foo");
  struct cvmfs_stat statValues1 = create_sample_stat("hello.world",
    10, 0770, 1, 1, content1.length(), xlist1);
  shash::Any meta1_hash = HashMeta(&statValues1);
  // FILE META 2
  XattrList *xlist2 = create_sample_xattrlist("bar");
  struct cvmfs_stat statValues2 = create_sample_stat("hello.world",
    10, 0770, 1, 1, content1.length(), xlist2);
  shash::Any meta2_hash = HashMeta(&statValues2);
  // FILE META 3
  XattrList *xlist3 = create_sample_xattrlist("foo");
  struct cvmfs_stat statValues3 = create_sample_stat("hello.world",
    10, 0700, 1, 1, content1.length(), xlist3);
  shash::Any meta3_hash = HashMeta(&statValues3);

  ASSERT_EQ(0, fs_traversal_instance_->interface->touch(
    context_,
    &content1_hash,
    &meta1_hash,
    &statValues1));
  ASSERT_EQ(0, fs_traversal_instance_->interface->touch(
    context_,
    &content1_hash,
    &meta2_hash,
    &statValues2));
  ASSERT_EQ(0, fs_traversal_instance_->interface->touch(
    context_,
    &content1_hash,
    &meta3_hash,
    &statValues3));
  ASSERT_EQ(0, fs_traversal_instance_->interface->touch(
    context_,
    &content2_hash,
    &meta1_hash,
    &statValues1));
  // Creating again should fail...
  ASSERT_EQ(-1, fs_traversal_instance_->interface->touch(
    context_,
    &content2_hash,
    &meta1_hash,
    &statValues1));
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

std::string dir = GetCurrentWorkingDirectory();
struct fs_traversal_test posix = {
  posix_get_interface(),
  dir.c_str(),
  ".data"
};

INSTANTIATE_TEST_CASE_P(PosixInterfaceTest,
                        T_Fs_Traversal_Interface,
                        ::testing::Values(&posix));
