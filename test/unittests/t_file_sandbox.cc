/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>
#include <string>

#include "c_file_sandbox.h"

#include "../../cvmfs/util.h"
#include "../../cvmfs/hash.h"
#include "../../cvmfs/prng.h"


class T_FileSandbox : public FileSandbox {
 protected:
  static const std::string sandbox_path;

 public:
  typedef std::vector<ExpectedHashString> ExpectedHashStrings;

  T_FileSandbox() :
    FileSandbox(T_FileSandbox::sandbox_path) {}

 protected:
  void SetUp() {
    CreateSandbox(T_FileSandbox::sandbox_path);
  }

  void TearDown() {
    RemoveSandbox(T_FileSandbox::sandbox_path);
  }

  shash::Any HashFile(const std::string &file_path) const {
    shash::Any sha_result(shash::kSha1);
    HashFileInternal(file_path, &sha_result);
    return sha_result;
  }

 private:
  /**
   * Wraps the call to shash::HashFile to create a check sum of the given file
   * googletest requires functions that have ASSERTs inside to return void, thus
   * we do a wrapper of the wrapper of the wrapper here :o)
   */
  void HashFileInternal(const std::string &file_path, shash::Any *digest) const {
    const bool retval = shash::HashFile(file_path, digest);
    ASSERT_TRUE(retval) << "failed to hash file: " << file_path;
  }
};

const std::string T_FileSandbox::sandbox_path = "/tmp/cvmfs_ut_filesandbox";


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//

TEST_F(T_FileSandbox, SandboxCreation) {
  EXPECT_TRUE(DirectoryExists(T_FileSandbox::sandbox_path));
}


TEST_F(T_FileSandbox, CreateRandomBufferMethod) {
  Prng rng;
  rng.InitSeed(27);

  const uint64_t buffer_size = 10 * 1024 * 1024;
  char *buffer = (char*)malloc(buffer_size);
  ASSERT_NE(static_cast<char*>(NULL), buffer);
  memset(buffer, 0, buffer_size);

  // count number of zero-bytes in the random buffer as 'checksum'
  CreateRandomBuffer(buffer, buffer_size, rng);
  uint32_t zeros = 0;
  for (uint64_t i = 0; i < buffer_size; ++i) {
    if (buffer[i] == 0) {
      ++zeros;
    }
  }

  EXPECT_EQ(41207u, zeros);
}


TEST_F(T_FileSandbox, EmptyFile) {
  const std::string empty_file = GetEmptyFile();
  const int64_t file_size = GetFileSize(empty_file);

  EXPECT_EQ(0, file_size) << "empty file was not empty";
}


TEST_F(T_FileSandbox, SmallFile) {
  const std::string small_file = GetSmallFile();
  const int64_t file_size = GetFileSize(small_file);

  EXPECT_EQ(50 * 1024, file_size) << "small file size does not match";

  shash::Any sha = HashFile(small_file);
  EXPECT_EQ("e86f148ca3a9a1ad9cf19979548e61c38bfa1384", sha.ToString());
}


TEST_F(T_FileSandbox, BigFile) {
  const std::string big_file = GetBigFile();
  const int64_t file_size = GetFileSize(big_file);

  EXPECT_EQ(4 * 1024 * 1024, file_size) << "big file size does not match";

  shash::Any sha = HashFile(big_file);
  EXPECT_EQ("59107e4c69e7687499423d3d85154fdba9cd8161", sha.ToString());
}


TEST_F(T_FileSandbox, HugeFileSlow) {
  const std::string huge_file = GetHugeFile();
  const int64_t file_size = GetFileSize(huge_file);

  EXPECT_EQ(100 * 1024 * 1024, file_size) << "huge file size does not match";

  shash::Any sha = HashFile(huge_file);
  EXPECT_EQ("e09bdb4354db2ac46309130ee91ad7c4131f29ea", sha.ToString());
}
