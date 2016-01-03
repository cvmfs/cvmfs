/**
 * This file is part of the CernVM File System.
 */

#include "gtest/gtest.h"


#include "../../cvmfs/compression.h"
#include "../../cvmfs/util.h"


namespace zlib {

// Test fixture that creates data structures necessary to test Compressor
class CompressorTest : public ::testing::Test {
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
  }

  virtual void TearDown() {
    delete buf;
    free(test_string);
  }

  char *test_string, *ptr_test_string;
  UniquePtr<Compressor> compressor;
  unsigned char *buf;
  size_t buf_size;
  size_t size_input;
};


TEST_F(CompressorTest, Compression) {
  compressor = zlib::Compressor::Construct(zlib::kZlibDefault);

  // Compress the output
  unsigned char *input = reinterpret_cast<unsigned char *>(ptr_test_string);
  bool deflate_finished =
    compressor->Deflate(true, &input, &size_input, &buf, &buf_size);

  ASSERT_TRUE(deflate_finished);
  ASSERT_TRUE(buf_size > 0);
  ASSERT_EQ(0U, size_input);

  // Decompress it, check if it's still the same
  char *decompress_buf;
  size_t decompress_size;
  DecompressMem2Mem(buf, buf_size,
    reinterpret_cast<void **>(decompress_buf), &decompress_size);

  // Check if the string is the same as the beginning
  ASSERT_EQ(0, strcmp(decompress_buf, test_string));

  free(decompress_buf);
}


TEST_F(CompressorTest, EchoCompression) {
  compressor = zlib::Compressor::Construct(zlib::kNoCompression);

  unsigned char *input = reinterpret_cast<unsigned char *>(ptr_test_string);
  bool deflate_finished =
    compressor->Deflate(true, &input, &size_input, &buf, &buf_size);

  ASSERT_TRUE(deflate_finished);
  ASSERT_GT(buf_size, 0);
  ASSERT_EQ(size_input, (size_t)0);

  // Make sure the compressed data is exactly the same as the
  // input.
  ASSERT_EQ(0, strcmp(reinterpret_cast<char *>(buf), test_string));
}

}  // end namespace zlib
