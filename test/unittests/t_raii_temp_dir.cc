/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "util/pointer.h"
#include "util/raii_temp_dir.h"

#include "platform.h"
#include "util/posix.h"

static bool DirExists(const std::string& dir) {
  platform_stat64 dir_stat;
  int ret = platform_stat(dir.c_str(), &dir_stat);
  return ret == 0;
}

class T_RaiiTempDir : public ::testing::Test {};

TEST_F(T_RaiiTempDir, Basic) {
  UniquePtr<RaiiTempDir> temp_dir(
      RaiiTempDir::Create(GetCurrentWorkingDirectory() + "/test_dir"));
  ASSERT_TRUE(temp_dir.IsValid());

  const std::string temp_path = temp_dir->dir();

  temp_dir.Destroy();

  ASSERT_FALSE(DirExists(temp_path));
}

TEST_F(T_RaiiTempDir, DeletedExternally) {
  UniquePtr<RaiiTempDir> temp_dir(
      RaiiTempDir::Create(GetCurrentWorkingDirectory() + "/test_dir"));
  ASSERT_TRUE(temp_dir.IsValid());

  const std::string temp_path = temp_dir->dir();

  RemoveTree(temp_path);

  ASSERT_FALSE(DirExists(temp_path));
}
