/**
 * This file is part of the CernVM File System.
 */

#include "gtest/gtest.h"


#include "../../cvmfs/compression.h"
#include "../../cvmfs/util.h"

// TODO(jblomer): typed tests

namespace zlib {

// Test fixture that creates data structures necessary to test Compressor
class T_Compressor : public ::testing::Test {
 protected:
  virtual void SetUp() {
    // Compress a known String
    test_string = strdup("Hello World!");
    ptr_test_string = test_string;

    // Include the null character
    size_input = strlen(test_string) + 1;

    // Create a buffer to hold the output
    buf = new unsigned char[100];
    buf_size = 100;

    long_size = 1024 * 1024 * 20;  // 20 MB
    long_string = new unsigned char[long_size];
  }

  virtual void TearDown() {
    delete long_string;
    delete buf;
    free(test_string);
  }

  char *test_string, *ptr_test_string;
  UniquePtr<Compressor> compressor;
  unsigned char *buf;
  size_t buf_size;
  size_t size_input;

  unsigned char *long_string;
  size_t long_size;
};


TEST_F(T_Compressor, Compression) {
  compressor = zlib::Compressor::Construct(zlib::kZlibDefault);

  // Compress the output
  unsigned char *input = reinterpret_cast<unsigned char *>(ptr_test_string);
  bool deflate_finished =
    compressor->Deflate(true, &input, &size_input, &buf, &buf_size);

  ASSERT_TRUE(deflate_finished);
  ASSERT_GT(buf_size, 0U);
  ASSERT_EQ(0U, size_input);

  // Decompress it, check if it's still the same
  char *decompress_buf;
  uint64_t decompress_size;
  DecompressMem2Mem(buf, buf_size,
    reinterpret_cast<void **>(&decompress_buf), &decompress_size);

  // Check if the string is the same as the beginning
  ASSERT_EQ(0, strcmp(decompress_buf, test_string));

  free(decompress_buf);
}


TEST_F(T_Compressor, CompressionLong) {
  compressor = zlib::Compressor::Construct(zlib::kZlibDefault);
  unsigned char *compress_buf =
    new unsigned char[compressor->DeflateBound(long_size)];
  unsigned compress_pos = 0;
  bool deflate_finished = false;
  unsigned char *input = long_string;
  size_t remaining = long_size;
  unsigned rounds = 0;

  while (!deflate_finished) {
    // Compress the output in multiple stages
    deflate_finished =
      compressor->Deflate(true, &input, &remaining, &buf, &buf_size);
    memcpy(compress_buf + compress_pos, buf, buf_size);
    compress_pos += buf_size;
    rounds++;
  }

  EXPECT_GT(rounds, 1U);
  EXPECT_GT(compress_pos, 0U);
  ASSERT_EQ(0U, remaining);

  // Decompress it, check if it's still the same
  char *decompress_buf;
  uint64_t decompress_size;
  bool retval = DecompressMem2Mem(compress_buf, compress_pos,
    reinterpret_cast<void **>(&decompress_buf), &decompress_size);
  EXPECT_EQ(true, retval);
  EXPECT_EQ(decompress_size, static_cast<uint64_t>(long_size));
  EXPECT_EQ(0, memcmp(decompress_buf, long_string, long_size));

  delete compress_buf;
  free(decompress_buf);
}


TEST_F(T_Compressor, EchoCompression) {
  compressor = zlib::Compressor::Construct(zlib::kNoCompression);

  unsigned char *input = reinterpret_cast<unsigned char *>(ptr_test_string);
  bool deflate_finished =
    compressor->Deflate(true, &input, &size_input, &buf, &buf_size);

  ASSERT_TRUE(deflate_finished);
  ASSERT_GT(buf_size, 0U);
  ASSERT_EQ(size_input, (size_t)0);

  // Make sure the compressed data is exactly the same as the
  // input.
  ASSERT_EQ(0, strcmp(reinterpret_cast<char *>(buf), test_string));
}


TEST_F(T_Compressor, EchoCompressionLong) {
  compressor = zlib::Compressor::Construct(zlib::kNoCompression);
  unsigned char *compress_buf =
    new unsigned char[compressor->DeflateBound(long_size)];
  unsigned compress_pos = 0;
  bool deflate_finished = false;
  unsigned char *input = long_string;
  size_t remaining = long_size;
  unsigned rounds = 0;

  while (!deflate_finished) {
    // Compress the output in multiple stages
    deflate_finished =
      compressor->Deflate(true, &input, &remaining, &buf, &buf_size);
    memcpy(compress_buf + compress_pos, buf, buf_size);
    compress_pos += buf_size;
    rounds++;
  }

  EXPECT_GT(rounds, 1U);
  EXPECT_GT(compress_pos, 0U);
  ASSERT_EQ(0U, remaining);

  EXPECT_EQ(compress_pos, long_size);
  EXPECT_EQ(0, memcmp(compress_buf, long_string, long_size));

  delete compress_buf;
}

}  // end namespace zlib
