/**
 * This file is part of the CernVM File System.
 */

#include <fcntl.h>
#include <gtest/gtest.h>

#include <string>

#include "util/file_guard.h"
#include "util/posix.h"

class T_FileGuard : public ::testing::Test {
 protected:
  static const std::string sandbox;

 protected:
  virtual void SetUp() {
    const bool retval = MkdirDeep(sandbox, 0700);
    ASSERT_TRUE(retval) << "failed to create sandbox";
  }

  virtual void TearDown() {
    const bool retval = RemoveTree(sandbox);
    ASSERT_TRUE(retval) << "failed to remove sandbox";
  }

  std::string GetFilename() const {
    const std::string path = CreateTempPath(sandbox + "/catalog", 0600);
    CheckEmpty(path);
    CheckExistence(path);
    return path;
  }

 protected:
  void CheckExistence(const std::string &path) const {
    ASSERT_TRUE(FileExists(path));
  }

 private:
  void CheckEmpty(const std::string &str) const { ASSERT_FALSE(str.empty()); }
};

const std::string T_FileGuard::sandbox = "./cvmfs_ut_unlink_guard";


TEST_F(T_FileGuard, Initialize) { }


TEST_F(T_FileGuard, SimpleUnlink) {
  const std::string file_path = GetFilename();
  {
    UnlinkGuard d(file_path);
    EXPECT_EQ(file_path, d.path());
    ASSERT_TRUE(FileExists(file_path));
    EXPECT_TRUE(d.IsEnabled());
  }
  EXPECT_FALSE(FileExists(file_path));
}


TEST_F(T_FileGuard, UnlinkDisable) {
  const std::string file_path = GetFilename();
  {
    UnlinkGuard d(file_path);
    EXPECT_EQ(file_path, d.path());
    ASSERT_TRUE(FileExists(file_path));
    EXPECT_TRUE(d.IsEnabled());
    d.Disable();
    ASSERT_TRUE(FileExists(file_path));
    EXPECT_FALSE(d.IsEnabled());
  }
  EXPECT_TRUE(FileExists(file_path));
  unlink(file_path.c_str());
  EXPECT_FALSE(FileExists(file_path));
}


TEST_F(T_FileGuard, UnlinkReenable) {
  const std::string file_path = GetFilename();
  {
    UnlinkGuard d(file_path);
    EXPECT_EQ(file_path, d.path());
    ASSERT_TRUE(FileExists(file_path));
    EXPECT_TRUE(d.IsEnabled());
    d.Disable();
    ASSERT_TRUE(FileExists(file_path));
    EXPECT_FALSE(d.IsEnabled());
    d.Enable();
    ASSERT_TRUE(FileExists(file_path));
    EXPECT_TRUE(d.IsEnabled());
  }
  EXPECT_FALSE(FileExists(file_path));
}


TEST_F(T_FileGuard, MultipleUnlinkGuards) {
  const std::string file_path1 = GetFilename();
  const std::string file_path2 = GetFilename();
  const std::string file_path3 = GetFilename();
  const std::string file_path4 = GetFilename();
  const std::string file_path5 = GetFilename();

  {
    ASSERT_TRUE(FileExists(file_path1));
    ASSERT_TRUE(FileExists(file_path2));
    ASSERT_TRUE(FileExists(file_path3));
    ASSERT_TRUE(FileExists(file_path4));
    ASSERT_TRUE(FileExists(file_path5));

    UnlinkGuard d1(file_path1);
    UnlinkGuard d2(file_path2);
    {
      UnlinkGuard d4(file_path4);
      { UnlinkGuard d5(file_path5); }

      EXPECT_FALSE(FileExists(file_path5));
    }
    EXPECT_FALSE(FileExists(file_path4));
    EXPECT_TRUE(FileExists(file_path1));
    EXPECT_TRUE(FileExists(file_path2));
    EXPECT_TRUE(FileExists(file_path3));

    UnlinkGuard d3(file_path3);
    EXPECT_TRUE(FileExists(file_path1));
    EXPECT_TRUE(FileExists(file_path2));
    EXPECT_TRUE(FileExists(file_path3));
  }

  EXPECT_FALSE(FileExists(file_path1));
  EXPECT_FALSE(FileExists(file_path2));
  EXPECT_FALSE(FileExists(file_path3));
}


TEST_F(T_FileGuard, UnlinkDeferredInit) {
  const std::string file_path = GetFilename();
  {
    ASSERT_TRUE(FileExists(file_path));

    UnlinkGuard d;
    EXPECT_FALSE(d.IsEnabled());
    EXPECT_TRUE(FileExists(file_path));

    d.Set(file_path);
    EXPECT_TRUE(d.IsEnabled());
    EXPECT_TRUE(FileExists(file_path));
  }
  EXPECT_FALSE(FileExists(file_path));
}


TEST_F(T_FileGuard, UnlinkDisabledInit) {
  const std::string file_path = GetFilename();
  {
    ASSERT_TRUE(FileExists(file_path));

    UnlinkGuard d(file_path, UnlinkGuard::kDisabled);
    EXPECT_FALSE(d.IsEnabled());
    EXPECT_TRUE(FileExists(file_path));
  }

  EXPECT_TRUE(FileExists(file_path));

  {
    ASSERT_TRUE(FileExists(file_path));

    UnlinkGuard d(file_path, UnlinkGuard::kDisabled);
    EXPECT_FALSE(d.IsEnabled());
    EXPECT_TRUE(FileExists(file_path));

    d.Enable();
    EXPECT_TRUE(d.IsEnabled());
    EXPECT_TRUE(FileExists(file_path));
  }
  EXPECT_FALSE(FileExists(file_path));
}


TEST_F(T_FileGuard, UnlinkExplicitEnabledInit) {
  const std::string file_path = GetFilename();
  {
    ASSERT_TRUE(FileExists(file_path));

    UnlinkGuard d(file_path, UnlinkGuard::kEnabled);
    EXPECT_TRUE(d.IsEnabled());
    EXPECT_TRUE(FileExists(file_path));

    d.Disable();
    EXPECT_FALSE(d.IsEnabled());
    EXPECT_TRUE(FileExists(file_path));
  }

  EXPECT_TRUE(FileExists(file_path));

  {
    ASSERT_TRUE(FileExists(file_path));

    UnlinkGuard d(file_path, UnlinkGuard::kEnabled);
    EXPECT_TRUE(d.IsEnabled());
    EXPECT_TRUE(FileExists(file_path));
  }
  EXPECT_FALSE(FileExists(file_path));
}


TEST_F(T_FileGuard, FdGuard) {
  int fd = -1;
  {
    fd = open("/dev/null", O_RDONLY);
    ASSERT_GE(fd, 0);
    FdGuard fd_guard(fd);
    int retval = fcntl(fd, F_GETFD, 0);
    EXPECT_NE(-1, retval);
  }
  int retval = fcntl(fd, F_GETFD, 0);
  EXPECT_EQ(-1, retval);
}
