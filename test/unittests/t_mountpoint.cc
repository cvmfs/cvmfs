/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <string>

#include "mountpoint.h"
#include "testutil.h"
#include "util/posix.h"

using namespace std;  // NOLINT

class T_MountPoint : public ::testing::Test {
 protected:
  virtual void SetUp() {
    used_fds_ = GetNoUsedFds();
    tmp_path_ = CreateTempDir("./cvmfs_ut_cache");
    file_system_ = NULL;
    mount_point_ = NULL;
  }

  virtual void TearDown() {
    if (tmp_path_ != "")
      RemoveTree(tmp_path_);
    EXPECT_EQ(used_fds_, GetNoUsedFds());
  }

 protected:
  FileSystem *file_system_;
  MountPoint *mount_point_;
  string tmp_path_;
  unsigned used_fds_;
};



TEST_F(T_MountPoint, FileSystem) {

}
