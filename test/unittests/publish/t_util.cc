/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <fcntl.h>

#include "publish/except.h"
#include "publish/repository_util.h"
#include "util/posix.h"

using namespace std;  // NOLINT

namespace publish {

class T_Util : public ::testing::Test {
 protected:
  virtual void SetUp() {
  }

 protected:
};

TEST_F(T_Util, CheckoutMarker) {
  EXPECT_EQ(NULL, CheckoutMarker::CreateFrom("/no/such/path"));

  shash::Any hash = shash::MkFromHexPtr(
    shash::HexPtr("0123456789abcdef0123456789abcdef01234567"),
    shash::kSuffixCatalog);
  CheckoutMarker m("tag", "branch", hash);
  m.SaveAs("cvmfs_test_checkout_marker");

  CheckoutMarker *l = CheckoutMarker::CreateFrom("cvmfs_test_checkout_marker");
  ASSERT_TRUE(l != NULL);
  EXPECT_EQ(m.tag(), l->tag());
  EXPECT_EQ(m.branch(), l->branch());
  EXPECT_EQ(m.hash(), l->hash());
  delete l;
}


TEST_F(T_Util, ServerLockFile) {
  EXPECT_FALSE(ServerLockFile::IsLocked("foo", true));
  EXPECT_TRUE(ServerLockFile::Acquire("foo", true));
  EXPECT_FALSE(ServerLockFile::Acquire("foo", true));
  EXPECT_TRUE(ServerLockFile::IsLocked("foo", true));
  ServerLockFile::Release("foo");
  EXPECT_FALSE(ServerLockFile::IsLocked("foo", true));

  pid_t pid_child = fork();
  ASSERT_TRUE(pid_child >= 0);
  if (pid_child == 0) {
    EXPECT_TRUE(ServerLockFile::Acquire("foo", true));
    exit(0);
  }
  EXPECT_EQ(0, WaitForChild(pid_child));

  EXPECT_TRUE(ServerLockFile::IsLocked("foo", true));
  EXPECT_FALSE(ServerLockFile::IsLocked("foo", false));
  EXPECT_FALSE(ServerLockFile::Acquire("foo", true));
  EXPECT_TRUE(ServerLockFile::Acquire("foo", false));
  EXPECT_TRUE(ServerLockFile::IsLocked("foo", false));
  ServerLockFile::Release("foo");
}


TEST_F(T_Util, SetInConfig) {
  EXPECT_THROW(SetInConfig("/no/such/file", "x", "y"), EPublish);
  EXPECT_FALSE(FileExists("test_publish_config"));
  SetInConfig("test_publish_config", "X", "y");
  EXPECT_TRUE(FileExists("test_publish_config"));

  int fd = open("test_publish_config", O_RDONLY);
  EXPECT_GE(fd, 0);
  std::string content;
  EXPECT_TRUE(SafeReadToString(fd, &content));
  close(fd);
  EXPECT_EQ("X=y\n", content);

  SetInConfig("test_publish_config", "X", "z");
  fd = open("test_publish_config", O_RDONLY);
  EXPECT_GE(fd, 0);
  EXPECT_TRUE(SafeReadToString(fd, &content));
  close(fd);
  EXPECT_EQ("X=z\n", content);

  SetInConfig("test_publish_config", "A", "b");
  fd = open("test_publish_config", O_RDONLY);
  EXPECT_GE(fd, 0);
  EXPECT_TRUE(SafeReadToString(fd, &content));
  close(fd);
  EXPECT_EQ("X=z\nA=b\n", content);
}

}  // namespace publish
