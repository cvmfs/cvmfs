/**
 * This file is part of the CernVM File System.
 */

#include <algorithm>
#include <cstdio>
#include <memory>
#include <vector>

#include "compression/compression.h"
#include "gtest/gtest.h"
#include "util/prng.h"
#include "util/smalloc.h"

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
    delete[] long_string;
    delete[] buf;
    free(test_string);
  }

  char *test_string, *ptr_test_string;
  std::unique_ptr<Compressor> compressor;
  unsigned char *buf;
  size_t buf_size;
  size_t size_input;

  unsigned char *long_string;
  size_t long_size;
};


TEST_F(T_Compressor, Compression) {
  compressor.reset(zlib::Compressor::Construct(zlib::kZlibDefault));

  // Compress the output
  unsigned char *input = reinterpret_cast<unsigned char *>(ptr_test_string);
  bool deflate_finished = compressor->Deflate(true, &input, &size_input, &buf,
                                              &buf_size);

  ASSERT_TRUE(deflate_finished);
  ASSERT_GT(buf_size, 0U);
  ASSERT_EQ(0U, size_input);

  // Decompress it, check if it's still the same
  char *decompress_buf;
  uint64_t decompress_size;
  DecompressMem2Mem(buf, buf_size, reinterpret_cast<void **>(&decompress_buf),
                    &decompress_size);

  // Check if the string is the same as the beginning
  ASSERT_EQ(0, strcmp(decompress_buf, test_string));

  free(decompress_buf);
}


TEST_F(T_Compressor, CompressionLong) {
  compressor.reset(zlib::Compressor::Construct(zlib::kZlibDefault));
  unsigned char
      *compress_buf = new unsigned char[compressor->DeflateBound(long_size)];
  unsigned compress_pos = 0;
  bool deflate_finished = false;
  unsigned char *input = long_string;
  size_t remaining = long_size;
  unsigned rounds = 0;

  while (!deflate_finished) {
    // Compress the output in multiple stages
    deflate_finished = compressor->Deflate(true, &input, &remaining, &buf,
                                           &buf_size);
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
                                  reinterpret_cast<void **>(&decompress_buf),
                                  &decompress_size);
  EXPECT_EQ(true, retval);
  EXPECT_EQ(decompress_size, static_cast<uint64_t>(long_size));
  EXPECT_EQ(0, memcmp(decompress_buf, long_string, long_size));

  delete[] compress_buf;
  free(decompress_buf);
}


TEST_F(T_Compressor, EchoCompression) {
  compressor.reset(zlib::Compressor::Construct(zlib::kNoCompression));

  unsigned char *input = reinterpret_cast<unsigned char *>(ptr_test_string);
  bool deflate_finished = compressor->Deflate(true, &input, &size_input, &buf,
                                              &buf_size);

  ASSERT_TRUE(deflate_finished);
  ASSERT_GT(buf_size, 0U);
  ASSERT_EQ(size_input, (size_t)0);

  // Make sure the compressed data is exactly the same as the
  // input.
  ASSERT_EQ(0, strcmp(reinterpret_cast<char *>(buf), test_string));
}


TEST_F(T_Compressor, EchoCompressionLong) {
  compressor.reset(zlib::Compressor::Construct(zlib::kNoCompression));
  std::unique_ptr<unsigned char> compress_buf(reinterpret_cast<unsigned char *>(
      smalloc(compressor->DeflateBound(long_size))));
  unsigned compress_pos = 0;
  bool deflate_finished = false;
  unsigned char *input = long_string;
  size_t remaining = long_size;
  unsigned rounds = 0;

  while (!deflate_finished) {
    // Compress the output in multiple stages
    deflate_finished = compressor->Deflate(true, &input, &remaining, &buf,
                                           &buf_size);
    memcpy(compress_buf.get() + compress_pos, buf, buf_size);
    compress_pos += buf_size;
    rounds++;
  }

  EXPECT_GT(rounds, 1U);
  EXPECT_GT(compress_pos, 0U);
  ASSERT_EQ(0U, remaining);

  EXPECT_EQ(compress_pos, long_size);
  EXPECT_EQ(0, memcmp(compress_buf.get(), long_string, long_size));
}


/**
 * Content hashes are computed over the compressed stream, so the deflate output
 * is part of cvmfs' on-disk format: a zlib that compresses differently (but
 * still correctly) renames every object. The build checks the zlib it selects
 * at configure time, this pins the one we actually end up linked against.
 *
 * Fingerprint and reference buffer are kept in sync with
 * externals/zlib/check_deflate_fingerprint.c. Every upstream zlib from 1.2.8 to
 * 1.3.1 produces this value; zlib-ng in compat mode does not.
 */
TEST(T_DeflateFingerprint, MatchesVendoredZlib) {
  const size_t kTotal = 64 * 1024;
  const size_t kInBlock = 4096 * 4;   // TaskRead::kBlockSize
  const size_t kOutBlock = 4096 * 2;  // TaskCompress::kCompressedBlockSize

  std::vector<unsigned char> data(kTotal);
  Prng prng;
  prng.InitSeed(1337);
  for (size_t i = 0; i < kTotal; ++i)
    data[i] = prng.Next(256);

  std::unique_ptr<Compressor> compressor(Compressor::Construct(kZlibDefault));
  std::vector<unsigned char> outbuf(kOutBlock);
  uLong crc = crc32(0L, Z_NULL, 0);
  size_t pos = 0;
  size_t total_out = 0;
  bool done = false;

  // mirrors TaskCompress::Process()
  while (!done) {
    const size_t in_size = std::min(kTotal - pos, kInBlock);
    const bool flush = (pos + in_size >= kTotal);
    unsigned char *in_ptr = &data[pos];
    size_t remaining_in = in_size;
    bool inner_done = false;
    pos += in_size;

    do {
      unsigned char *out_ptr = &outbuf[0];
      size_t produced = kOutBlock;
      inner_done = compressor->Deflate(flush, &in_ptr, &remaining_in, &out_ptr,
                                       &produced);
      crc = crc32(crc, &outbuf[0], static_cast<uInt>(produced));
      total_out += produced;
    } while ((remaining_in > 0) || (flush && !inner_done));

    if (flush)
      done = true;
  }

  char fingerprint[64];
  snprintf(fingerprint, sizeof(fingerprint), "%08lx:%lu",
           static_cast<unsigned long>(crc),
           static_cast<unsigned long>(total_out));
  EXPECT_STREQ("e415c33f:65562", fingerprint)
      << "this zlib does not compress the way cvmfs expects, most likely "
      << "zlib-ng in zlib-compat mode (" << zlibVersion() << "). Objects "
      << "written by this build get different content hashes, which breaks "
      << "deduplication against published repositories.";
}

}  // end namespace zlib
