/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "export_plugin/fs_traversal_interface.h"
#include "export_plugin/fs_traversal_posix.h"

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
    delete fs_traversal_instance_->repo;
  }
 protected:
  struct fs_traversal_test *fs_traversal_instance_;
  struct fs_traversal_context *context_;
};

TEST_P(T_Fs_Traversal_Interface, CreateFile) {
  std::string content = "Hello world!\nHello traversal!";
  shash::Any content_hash;
  shash::HashString(content, &content_hash);
}

struct fs_traversal_test posix = {
  posix_get_interface(),
  get_current_dir_name(),
  ".data"
};

INSTANTIATE_TEST_CASE_P(PosixInterfaceTest,
                        T_Fs_Traversal_Interface,
                        ::testing::Values(&posix));
