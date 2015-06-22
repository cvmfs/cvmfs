/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <pthread.h>

#include <cstdio>
#include <string>

#include "../../cvmfs/hash.h"
#include "../../cvmfs/manifest.h"
#include "../../cvmfs/util.h"

using namespace std;  // NOLINT

namespace manifest {

class T_Manifest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    tmp_path_ = CreateTempDir("/tmp/cvmfs-test");
    EXPECT_NE("", tmp_path_);
  }

  virtual void TearDown() {
    if (tmp_path_ != "")
      RemoveTree(tmp_path_);
  }

 protected:
  string tmp_path_;
};


TEST_F(T_Manifest, ReadChecksum) {
  shash::Any retrieved_hash;
  uint64_t last_modified = 0;
  EXPECT_FALSE(
    Manifest::ReadChecksum("test", tmp_path_, &retrieved_hash, &last_modified));

  shash::Any rnd_hash(shash::kRmd160);
  rnd_hash.Randomize();
  FILE *f = fopen((tmp_path_ + "/cvmfschecksum.test").c_str(), "w");
  ASSERT_TRUE(f != NULL);

  EXPECT_FALSE(
    Manifest::ReadChecksum("test", tmp_path_, &retrieved_hash, &last_modified));

  fwrite(rnd_hash.ToString().data(), 1, rnd_hash.ToString().length(), f);
  fflush(f);
  EXPECT_FALSE(
    Manifest::ReadChecksum("test", tmp_path_, &retrieved_hash, &last_modified));

  fwrite("T", 1, 1, f);
  fflush(f);
  EXPECT_FALSE(
    Manifest::ReadChecksum("test", tmp_path_, &retrieved_hash, &last_modified));

  fwrite("42", 2, 1, f);
  fflush(f);
  EXPECT_TRUE(
    Manifest::ReadChecksum("test", tmp_path_, &retrieved_hash, &last_modified));
  EXPECT_EQ(rnd_hash, retrieved_hash);
  EXPECT_EQ(42U, last_modified);

  fclose(f);
}

}  // namespace manifest
