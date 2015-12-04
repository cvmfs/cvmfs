/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <fcntl.h>

#include "../../cvmfs/compression.h"
#include "../../cvmfs/hash.h"

TEST(T_Compression, CompressFd2Null) {
  shash::Any hash(shash::kSha1);
  uint64_t size = 42;
  int fd_null = open("/dev/null", O_RDONLY);
  EXPECT_GE(fd_null, 0);
  EXPECT_TRUE(zlib::CompressFd2Null(fd_null, &hash, &size));
  EXPECT_EQ(0U, size);
  EXPECT_EQ("e8ec3d88b62ebf526e4e5a4ff6162a3aa48a6b78", hash.ToString());
  close(fd_null);

  EXPECT_FALSE(zlib::CompressFd2Null(-1, &hash));
}
