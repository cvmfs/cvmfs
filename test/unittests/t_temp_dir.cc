/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <util/pointer.h>
#include <util/temp_dir.h>

#include "platform.h"
#include "util/posix.h"

static bool DirExists(const std::string& dir) {
  platform_stat64 dir_stat;
  int ret = platform_stat(dir.c_str(), &dir_stat);
  return ret == 0;
}

class T_TempDir : public ::testing::Test {};

TEST_F(T_TempDir, Basic) {
  UniquePtr<TempDir> temp_dir(TempDir::Create("/tmp/test_dir"));
  ASSERT_TRUE(temp_dir.IsValid());

  const std::string temp_path = temp_dir->dir();

  temp_dir.Destroy();

  ASSERT_FALSE(DirExists(temp_path));
}

TEST_F(T_TempDir, DeleteWhileBusy) {
  UniquePtr<TempDir> temp_dir(TempDir::Create("/tmp/test_dir"));
  ASSERT_TRUE(temp_dir.IsValid());

  const std::string temp_path = temp_dir->dir();

  const std::string test_file = temp_path + "/test_file";

  CreateFile(test_file, 0600);
  int fd = open(test_file.c_str(), O_APPEND);

  temp_dir.Destroy();

  ASSERT_FALSE(DirExists(temp_path));

  int ret = close(fd);

  ASSERT_EQ(0, ret);
}
