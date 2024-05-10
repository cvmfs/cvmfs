/**
 * This file is part of the CernVM File System.
 */

#include "gtest/gtest.h"

#include <cstdlib>  // for rand()

#include "c_file_sandbox.h"
#include "compression/compression.h"
#include "compression/input_mem.h"
#include "compression/input_path.h"
#include "network/sink.h"
#include "network/sink_mem.h"
#include "network/sink_path.h"
#include "util/pointer.h"
#include "util/smalloc.h"

// TODO(jblomer): typed tests

namespace zlib {

// Test fixture that creates data structures necessary to test Compressor
class T_Compressor : public FileSandbox {
 public:
  T_Compressor() : FileSandbox(std::string(sandbox_path)) {}

 protected:
  virtual void SetUp() {
    CreateSandbox();

    // Compress a known String
    test_string = strdup("Hello World!");
    ptr_test_string = test_string;
    str_test_string = test_string;

    // Include the null character
    size_input = strlen(test_string) + 1;

    // Create a buffer to hold the output
    buf = new unsigned char[100];
    buf_size = 100;

    long_size = 1024 * 1024 * 20;  // 20 MB
    long_string = new unsigned char[long_size];
  }

  virtual void TearDown() {
    RemoveSandbox();

    delete[] long_string;
    delete[] buf;
    free(test_string);
  }

  char *test_string, *ptr_test_string;
  std::string str_test_string;
  UniquePtr<Compressor> compressor;
  unsigned char *buf;
  size_t buf_size;
  size_t size_input;

  unsigned char *long_string;
  size_t long_size;

  static const char sandbox_path[];
};

const char T_Compressor::sandbox_path[] = "./cvmfs_ut_compressor";


TEST_F(T_Compressor, CompressionSinkMem2Mem) {
  compressor = zlib::Compressor::Construct(zlib::kZlibDefault);

  // Compress the output
  // unsigned char *input = reinterpret_cast<unsigned char *>(ptr_test_string);

  zlib::InputMem in(reinterpret_cast<const unsigned char*>(
                                                       str_test_string.c_str()),
                    str_test_string.size(), 16384);
  cvmfs::MemSink out(0);

  zlib::StreamStates res = compressor->CompressStream(&in, &out);

  EXPECT_EQ(res, zlib::kStreamEnd);
  EXPECT_GT(out.pos(), 0U);

  // Decompress it, check if it's still the same
  char *decompress_buf;
  uint64_t decompress_size;
  DecompressMem2Mem(out.data(), out.pos(),
    reinterpret_cast<void **>(&decompress_buf), &decompress_size);

  // Check if the string is the same as the beginning
  // (decompress_buf is not null terminated --> only compared within size)
  EXPECT_EQ(str_test_string.size(), decompress_size);
  EXPECT_EQ(0, strncmp(decompress_buf, str_test_string.c_str(),
                       str_test_string.size()));

  free(decompress_buf);
}

TEST_F(T_Compressor, CompressionSinkMem2MemLarge) {
  compressor = zlib::Compressor::Construct(zlib::kZlibDefault);

  // Compress the output
  size_t in_size = 16384;
  size_t chunk_size = 8000;

  char letters[] = "abcdefghijklmnopqrstuvwxyz";
  unsigned char *input = static_cast<unsigned char *>(smalloc(in_size));

  // random filling of letters
  for (size_t i = 0; i < in_size; i++) {
    input[i] = letters[rand() % 26];
  }

  zlib::InputMem in(input, in_size, chunk_size);
  cvmfs::MemSink out(in_size);

  zlib::StreamStates res = compressor->CompressStream(&in, &out);

  ASSERT_EQ(res, zlib::kStreamEnd);
  ASSERT_GT(out.pos(), 0U);

  // Decompress it, check if it's still the same
  char *decompress_buf;
  uint64_t decompress_size;
  EXPECT_TRUE(DecompressMem2Mem(out.data(), out.pos(),
    reinterpret_cast<void **>(&decompress_buf), &decompress_size));

  EXPECT_EQ(in_size, decompress_size);

  // Check if the string is the same as the beginning
  ASSERT_EQ(0, memcmp(decompress_buf, input, in_size));

  free(decompress_buf);
  free(input);
}

// Also tests Input_File and SinkFile because *Path uses it under the hood
TEST_F(T_Compressor, CompressionSinkPath2PathLarge) {
  compressor = zlib::Compressor::Construct(zlib::kZlibDefault);
  size_t in_size = 16384*3;  // make large than decomp buffer size (32 KB)
  size_t chunk_size = 8000;

  char letters[] = "abcdefghijklmnopqrstuvwxyz";
  unsigned char *in_buf = static_cast<unsigned char *>(smalloc(in_size));

  // random filling of letters
  for (size_t i = 0; i < in_size; i++) {
    in_buf[i] = letters[rand() % 26];
  }

  std::string in_path;
  FILE *in_f = CreateTempFile(sandbox_path, 0600, "w+", &in_path);
  fwrite(in_buf, 1, in_size, in_f);
  fclose(in_f);
  zlib::InputPath input(in_path, chunk_size);

  std::string out_path;
  FILE *out_f = CreateTempFile(sandbox_path, 0600, "w+", &out_path);
  fclose(out_f);

  cvmfs::PathSink out(out_path);

  // Compress the output
  zlib::StreamStates res = compressor->CompressStream(&input, &out);

  EXPECT_EQ(res, zlib::kStreamEnd);

  std::string decompress_path;
  FILE *decompress_f =
                     CreateTempFile(sandbox_path, 0600, "w+", &decompress_path);
  fclose(decompress_f);

  // Decompress it, check if it's still the same
  EXPECT_TRUE(DecompressPath2Path(out_path, decompress_path));

  decompress_f = fopen(decompress_path.c_str(), "rb");

  // get file sizes; read decompressed file into buffer
  fseek(decompress_f, 0L, SEEK_END);
  size_t decompress_size = ftell(decompress_f);

  unsigned char *decompress_buf =
                          static_cast<unsigned char*>(smalloc(decompress_size));
  fseek(decompress_f, 0L, SEEK_SET);
  EXPECT_GT(fread(decompress_buf, 1, decompress_size, decompress_f), 0ul);
  fclose(decompress_f);

  // Check if decompressed content is equal to original one
  EXPECT_EQ(in_size, decompress_size);
  EXPECT_EQ(0, memcmp(decompress_buf, in_buf, in_size));

  free(decompress_buf);
  free(in_buf);
}

TEST_F(T_Compressor, EchoCompressionSinkMem2MemLarge) {
  compressor = zlib::Compressor::Construct(zlib::kNoCompression);

  // Compress the output
  size_t in_size = 16384;
  size_t chunk_size = 8000;

  char letters[] = "abcdefghijklmnopqrstuvwxyz";
  unsigned char *input = static_cast<unsigned char *>(smalloc(in_size));

  // random filling of letters
  for (size_t i = 0; i < in_size; i++) {
    input[i] = letters[rand() % 26];
  }

  zlib::InputMem in(input, in_size, chunk_size);
  cvmfs::MemSink out(in_size);

  zlib::StreamStates res = compressor->CompressStream(&in, &out);

  EXPECT_EQ(res, zlib::kStreamEnd);

  // Check if decompressed content is equal to original one
  EXPECT_EQ(0, memcmp(out.data(), input, in_size));

  free(input);
}

// Also tests Input_File and SinkFile because *Path uses it under the hood
TEST_F(T_Compressor, EchoCompressionSinkPath2PathLarge) {
  compressor = zlib::Compressor::Construct(zlib::kNoCompression);
  size_t in_size = 16384*3;  // make large than decomp buffer size (32 KB)
  size_t chunk_size = 8000;

  char letters[] = "abcdefghijklmnopqrstuvwxyz";
  unsigned char *in_buf = static_cast<unsigned char *>(smalloc(in_size));

  // random filling of letters
  for (size_t i = 0; i < in_size; i++) {
    in_buf[i] = letters[rand() % 26];
  }

  std::string in_path;
  FILE *in_f = CreateTempFile(sandbox_path, 0600, "w+", &in_path);
  fwrite(in_buf, 1, in_size, in_f);
  fclose(in_f);
  zlib::InputPath input(in_path, chunk_size);

  std::string out_path;
  FILE *out_f = CreateTempFile(sandbox_path, 0600, "w+", &out_path);
  fclose(out_f);

  cvmfs::PathSink out(out_path);

  // Compress the output
  zlib::StreamStates res = compressor->CompressStream(&input, &out);

  EXPECT_EQ(res, zlib::kStreamEnd);

  out_f = fopen(out_path.c_str(), "rb");

  // get file sizes; read decompressed file into buffer
  fseek(out_f, 0L, SEEK_END);
  size_t out_size = ftell(out_f);

  unsigned char *out_buf = static_cast<unsigned char*>(smalloc(out_size));
  fseek(out_f, 0L, SEEK_SET);
  EXPECT_GT(fread(out_buf, 1, out_size, out_f), 0ul);
  fclose(out_f);

  // Check if decompressed content is equal to original one
  EXPECT_EQ(in_size, out_size);
  EXPECT_EQ(0, memcmp(out_buf, in_buf, in_size));

  free(out_buf);
  free(in_buf);
}


TEST_F(T_Compressor, Compression) {
  compressor = zlib::Compressor::Construct(zlib::kZlibDefault);

  // Compress the output
  unsigned char *input = reinterpret_cast<unsigned char *>(ptr_test_string);
  bool deflate_finished =
    compressor->CompressStream(true, &input, &size_input, &buf, &buf_size);

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
    new unsigned char[compressor->CompressUpperBound(long_size)];
  unsigned compress_pos = 0;
  bool deflate_finished = false;
  unsigned char *input = long_string;
  size_t remaining = long_size;
  unsigned rounds = 0;

  while (!deflate_finished) {
    // Compress the output in multiple stages
    deflate_finished =
      compressor->CompressStream(true, &input, &remaining, &buf, &buf_size);
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

  delete[] compress_buf;
  free(decompress_buf);
}


TEST_F(T_Compressor, EchoCompression) {
  compressor = zlib::Compressor::Construct(zlib::kNoCompression);

  unsigned char *input = reinterpret_cast<unsigned char *>(ptr_test_string);
  bool deflate_finished =
    compressor->CompressStream(true, &input, &size_input, &buf, &buf_size);

  ASSERT_TRUE(deflate_finished);
  ASSERT_GT(buf_size, 0U);
  ASSERT_EQ(size_input, (size_t)0);

  // Make sure the compressed data is exactly the same as the
  // input.
  ASSERT_EQ(0, strcmp(reinterpret_cast<char *>(buf), test_string));
}


TEST_F(T_Compressor, EchoCompressionLong) {
  compressor = zlib::Compressor::Construct(zlib::kNoCompression);
  UniquePtr<unsigned char> compress_buf(reinterpret_cast<unsigned char *>(
    smalloc(compressor->CompressUpperBound(long_size))));
  unsigned compress_pos = 0;
  bool deflate_finished = false;
  unsigned char *input = long_string;
  size_t remaining = long_size;
  unsigned rounds = 0;

  while (!deflate_finished) {
    // Compress the output in multiple stages
    deflate_finished =
      compressor->CompressStream(true, &input, &remaining, &buf, &buf_size);
    memcpy(compress_buf.weak_ref() + compress_pos, buf, buf_size);
    compress_pos += buf_size;
    rounds++;
  }

  EXPECT_GT(rounds, 1U);
  EXPECT_GT(compress_pos, 0U);
  ASSERT_EQ(0U, remaining);

  EXPECT_EQ(compress_pos, long_size);
  EXPECT_EQ(0, memcmp(compress_buf.weak_ref(), long_string, long_size));
}

}  // end namespace zlib
