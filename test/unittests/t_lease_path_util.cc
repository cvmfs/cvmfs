/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "receiver/lease_path_util.h"
#include "shortstring.h"

class T_LeasePathUtil : public ::testing::Test {};

TEST_F(T_LeasePathUtil, RootLease) {
  const PathString lease("");
  const PathString path1("");
  const PathString path2("a");
  const PathString path3("b");

  ASSERT_TRUE(receiver::IsSubPath(lease, path1));
  ASSERT_TRUE(receiver::IsSubPath(lease, path2));
  ASSERT_TRUE(receiver::IsSubPath(lease, path3));
}

TEST_F(T_LeasePathUtil, Identical) {
  const PathString lease("lease-dir");
  const PathString path("lease-dir");

  ASSERT_TRUE(receiver::IsSubPath(lease, path));
}

TEST_F(T_LeasePathUtil, SubPathLease) {
  const PathString lease("sub/path");
  const PathString path1("sub/path/subdir");
  const PathString path2("sub/path_with_same_prefix");
  const PathString path3("sub_path_with_similar_prefix");
  const PathString empty_path("");

  const PathString path_above_lease("sub");

  ASSERT_TRUE(receiver::IsSubPath(lease, path1));
  ASSERT_FALSE(receiver::IsSubPath(lease, path2));
  ASSERT_FALSE(receiver::IsSubPath(lease, path3));
  ASSERT_FALSE(receiver::IsSubPath(lease, empty_path));

  ASSERT_FALSE(receiver::IsSubPath(lease, path_above_lease));
}
