/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>
#include <pthread.h>

#include <cstdio>
#include <string>

#include "crypto/hash.h"
#include "manifest.h"
#include "util/posix.h"

using namespace std;  // NOLINT

namespace manifest {

class T_Manifest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    tmp_path_ = CreateTempDir("./cvmfs_ut_manifest");
    EXPECT_NE("", tmp_path_);
  }

  virtual void TearDown() {
    if (tmp_path_ != "")
      RemoveTree(tmp_path_);
  }

 protected:
  string tmp_path_;
};


TEST_F(T_Manifest, Breadcrumb) {
  EXPECT_FALSE(Breadcrumb().IsValid());
  EXPECT_FALSE(Breadcrumb(shash::Any(shash::kSha1), 0, 0).IsValid());
  EXPECT_FALSE(Breadcrumb(shash::Any(shash::kShake128), 1, 0).IsValid());
  shash::Any rnd_hash(shash::kRmd160);
  rnd_hash.Randomize();
  EXPECT_FALSE(Breadcrumb(rnd_hash, 0, 0).IsValid());
  EXPECT_FALSE(Breadcrumb(rnd_hash, 1, -1ul).IsValid());
  EXPECT_TRUE(Breadcrumb(rnd_hash, 1, 0).IsValid());

  EXPECT_FALSE(Breadcrumb("").IsValid());
  EXPECT_FALSE(Breadcrumb("0").IsValid());
  EXPECT_FALSE(Breadcrumb("T").IsValid());
  EXPECT_FALSE(Breadcrumb("TTT").IsValid());
  EXPECT_FALSE(Breadcrumb("T0").IsValid());
  EXPECT_FALSE(Breadcrumb("T1").IsValid());
  EXPECT_FALSE(Breadcrumb("0T").IsValid());
  EXPECT_TRUE(
      Breadcrumb("0000000000000000000000000000000000000001T1").IsValid());
  EXPECT_TRUE(
      Breadcrumb("0000000000000000000000000000000000000001T1R10").IsValid());
  EXPECT_FALSE(
      Breadcrumb("0000000000000000000000000000000000000001T0").IsValid());
}


TEST_F(T_Manifest, ReadBreadcrumb) {
  EXPECT_FALSE(Manifest::ReadBreadcrumb("test", tmp_path_).IsValid());

  shash::Any rnd_hash(shash::kRmd160);
  rnd_hash.Randomize();
  FILE *f = fopen((tmp_path_ + "/cvmfschecksum.test").c_str(), "w");
  ASSERT_TRUE(f != NULL);

  EXPECT_FALSE(Manifest::ReadBreadcrumb("test", tmp_path_).IsValid());

  fwrite(rnd_hash.ToString().data(), 1, rnd_hash.ToString().length(), f);
  fflush(f);
  EXPECT_FALSE(Manifest::ReadBreadcrumb("test", tmp_path_).IsValid());

  fwrite("T", 1, 1, f);
  fflush(f);
  EXPECT_FALSE(Manifest::ReadBreadcrumb("test", tmp_path_).IsValid());

  fwrite("42", 2, 1, f);
  fflush(f);

  Breadcrumb breadcrumb = Manifest::ReadBreadcrumb("test", tmp_path_);
  EXPECT_TRUE(breadcrumb.IsValid());
  EXPECT_TRUE(breadcrumb.IsValid());
  EXPECT_EQ(rnd_hash, breadcrumb.catalog_hash);
  EXPECT_EQ(42U, breadcrumb.timestamp);

  fclose(f);
}

}  // namespace manifest
