/**
 * This file is part of the CernVM File System.
 */

#include <algorithm>
#include <cstdio>
#include <vector>

#include "compression/compression.h"
#include "gtest/gtest.h"

#include <cstdlib>  // for rand()

#include "c_file_sandbox.h"
#include "compression/compressor.h"
#include "compression/compressor_zlib.h"
#include "compression/decompressor.h"
#include "compression/input_mem.h"
#include "compression/input_path.h"
#include "network/sink.h"
#include "network/sink_mem.h"
#include "network/sink_path.h"
#include "util/pointer.h"
#include "util/prng.h"
#include "util/smalloc.h"

// TODO(jblomer): typed tests

namespace zip {

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

  void ExerciseCompressionLongNewOutbufTooSmall(zip::Algorithm alg);

  char *test_string, *ptr_test_string;
  std::string str_test_string;
  unsigned char *buf;
  size_t buf_size;
  size_t size_input;

  unsigned char *long_string;
  size_t long_size;

  static const char sandbox_path[];
};

const char T_Compressor::sandbox_path[] = "./cvmfs_ut_compressor";

TEST_F(T_Compressor, ZstdCompressionSinkMem2Mem) {
  UniquePtr<Compressor> compressor;
  UniquePtr<Decompressor> decompressor;
  compressor = zip::Compressor::Construct(zip::kZstd);
  decompressor = zip::Decompressor::Construct(zip::kZstd);

  // Compress the output
  // unsigned char *input = reinterpret_cast<unsigned char *>(ptr_test_string);

  zip::InputMem in(reinterpret_cast<const unsigned char*>(
                                                       str_test_string.c_str()),
                    str_test_string.size(), 16384);
  cvmfs::MemSink out(0);
  const zip::StreamStates res = compressor->Compress(&in, &out);

  EXPECT_EQ(res, zip::kStreamEnd);
  EXPECT_GT(out.pos(), 0U);

  zip::InputMem compress(out.data(), out.pos());

  cvmfs::MemSink decompress_out(0);

  EXPECT_EQ(decompressor->DecompressStream(&compress, &decompress_out),
                                                               zip::kStreamEnd);

  // Check if the data is the same as the beginning
  // (decompress_buf is not null terminated --> only compared within size)
  EXPECT_EQ(str_test_string.size(), decompress_out.pos());
  ASSERT_EQ(0, memcmp(decompress_out.data(), str_test_string.c_str(),
                                                       str_test_string.size()));
}


TEST_F(T_Compressor, CompressionSinkMem2Mem) {
  UniquePtr<Compressor> compressor;
  UniquePtr<Decompressor> decompressor;
  compressor = zip::Compressor::Construct(zip::kZlib);
  decompressor = zip::Decompressor::Construct(zip::kZlib);

  // Compress the output
  // unsigned char *input = reinterpret_cast<unsigned char *>(ptr_test_string);

  zip::InputMem in(reinterpret_cast<const unsigned char*>(
                                                       str_test_string.c_str()),
                    str_test_string.size(), 16384);
  cvmfs::MemSink out(0);

  const zip::StreamStates res = compressor->Compress(&in, &out);

  EXPECT_EQ(res, zip::kStreamEnd);
  EXPECT_GT(out.pos(), 0U);

  zip::InputMem compress(out.data(), out.pos());

  cvmfs::MemSink decompress_out(0);
  EXPECT_EQ(decompressor->DecompressStream(&compress, &decompress_out),
                                                               zip::kStreamEnd);


  // Check if the data is the same as the beginning
  // (decompress_buf is not null terminated --> only compared within size)
  EXPECT_EQ(str_test_string.size(), decompress_out.pos());
  ASSERT_EQ(0, memcmp(decompress_out.data(), str_test_string.c_str(),
                                                       str_test_string.size()));
}

TEST_F(T_Compressor, ZstdCompressionAndSplitDecompressionSinkMem2MemLarge) {
  UniquePtr<Compressor> compressor;
  UniquePtr<Decompressor> decompressor;
  compressor = zip::Compressor::Construct(zip::kZstd);
  decompressor = zip::Decompressor::Construct(zip::kZstd);

  // Compress the output
  const size_t in_size = 16384;
  const size_t chunk_size = 8000;

  const char letters[] = "abcdefghijklmnopqrstuvwxyz";
  unsigned char *input = static_cast<unsigned char *>(smalloc(in_size));

  // random filling of letters
  for (size_t i = 0; i < in_size; i++) {
    input[i] = letters[rand() % 26];
  }


  zip::InputMem in(input, in_size, chunk_size);
  cvmfs::MemSink out(in_size);

  const zip::StreamStates res = compressor->Compress(&in, &out);

  ASSERT_EQ(res, zip::kStreamEnd);
  ASSERT_GT(out.pos(), 0U);

  // Decompress in chunks
  zip::InputMem compress1(out.data(), out.pos() / 2);
  const size_t size_rest = out.pos() / 2 + out.pos() % 2;
  zip::InputMem compress2(out.data() + out.pos() / 2, size_rest);
  cvmfs::MemSink decompress_out(0);
  EXPECT_EQ(decompressor->DecompressStream(&compress1, &decompress_out),
                                                          zip::kStreamContinue);
  EXPECT_EQ(decompressor->DecompressStream(&compress2, &decompress_out),
                                                               zip::kStreamEnd);

  // Check if the data is the same as the beginning
  EXPECT_EQ(in_size, decompress_out.pos());
  EXPECT_EQ(0, memcmp(decompress_out.data(), input, in_size));

  free(input);
}

TEST_F(T_Compressor, CompressionAndSplitDecompressionSinkMem2MemLarge) {
  UniquePtr<Compressor> compressor;
  UniquePtr<Decompressor> decompressor;
  compressor = zip::Compressor::Construct(zip::kZlib);
  decompressor = zip::Decompressor::Construct(zip::kZlib);

  // Compress the output
  const size_t in_size = 16384;
  const size_t chunk_size = 8000;

  const char letters[] = "abcdefghijklmnopqrstuvwxyz";
  unsigned char *input = static_cast<unsigned char *>(smalloc(in_size));

  // random filling of letters
  for (size_t i = 0; i < in_size; i++) {
    input[i] = letters[rand() % 26];
  }

  zip::InputMem in(input, in_size, chunk_size);
  cvmfs::MemSink out(in_size);

  const zip::StreamStates res = compressor->Compress(&in, &out);

  ASSERT_EQ(res, zip::kStreamEnd);
  ASSERT_GT(out.pos(), 0U);

  // Decompress in chunks
  zip::InputMem compress1(out.data(), out.pos() / 2);
  const size_t size_rest = out.pos() / 2 + out.pos() % 2;
  zip::InputMem compress2(out.data() + out.pos() / 2, size_rest);

  cvmfs::MemSink decompress_out(0);
  EXPECT_EQ(decompressor->DecompressStream(&compress1, &decompress_out),
                                                          zip::kStreamContinue);
  EXPECT_EQ(decompressor->DecompressStream(&compress2, &decompress_out),
                                                               zip::kStreamEnd);


  // Check if the data is the same as the beginning
  EXPECT_EQ(in_size, decompress_out.pos());
  EXPECT_EQ(0, memcmp(decompress_out.data(), input, in_size));

  free(input);
}

TEST_F(T_Compressor, ZstdCompressionSinkMemNull2Mem) {
  UniquePtr<Compressor> compressor;
  UniquePtr<Decompressor> decompressor;
  compressor = zip::Compressor::Construct(zip::kZstd);
  decompressor = zip::Decompressor::Construct(zip::kZstd);

  const size_t chunk_size = 8000;
  const size_t in_size = 0;

  zip::InputMem in(NULL, in_size, chunk_size);
  cvmfs::MemSink out(0);

  const zip::StreamStates res = compressor->Compress(&in, &out);

  ASSERT_EQ(res, zip::kStreamEnd);
  ASSERT_GT(out.pos(), 0U);

  shash::Any file_hash(shash::kSha1);
  shash::HashMem(out.data(), out.pos(), &file_hash);

  EXPECT_EQ(file_hash.ToString(),
            "fb2e51cbd24e286dd066bd419d77cd772967e384");

  // Decompress it, check if it's still the same
  zip::InputMem compress(out.data(), out.pos());
  cvmfs::MemSink decompress_out(0);
  EXPECT_EQ(decompressor->DecompressStream(&compress, &decompress_out),
                                                               zip::kStreamEnd);

  EXPECT_EQ(in_size, decompress_out.pos());
}

TEST_F(T_Compressor, CompressionSinkMemNull2Mem) {
  UniquePtr<Compressor> compressor;
  UniquePtr<Decompressor> decompressor;
  compressor = zip::Compressor::Construct(zip::kZlib);
  decompressor = zip::Decompressor::Construct(zip::kZlib);

  const size_t chunk_size = 8000;
  const size_t in_size = 0;

  zip::InputMem in(NULL, in_size, chunk_size);
  cvmfs::MemSink out(0);

  const zip::StreamStates res = compressor->Compress(&in, &out);

  ASSERT_EQ(res, zip::kStreamEnd);
  ASSERT_GT(out.pos(), 0U);

  shash::Any file_hash(shash::kSha1);
  shash::HashMem(out.data(), out.pos(), &file_hash);

  EXPECT_EQ(file_hash.ToString(),
            "e8ec3d88b62ebf526e4e5a4ff6162a3aa48a6b78");

  // Decompress it, check if it's still the same
  zip::InputMem compress(out.data(), out.pos());
  cvmfs::MemSink decompress_out(0);
  EXPECT_EQ(decompressor->DecompressStream(&compress, &decompress_out),
                                                               zip::kStreamEnd);

  EXPECT_EQ(in_size, decompress_out.pos());
}

TEST_F(T_Compressor, ZstdCompressionSinkPath2PathLarge) {
  UniquePtr<Compressor> compressor;
  UniquePtr<Decompressor> decompressor;
  compressor = zip::Compressor::Construct(zip::kZstd);
  decompressor = zip::Decompressor::Construct(zip::kZstd);

  const size_t in_size = 16384 * 3ul;  // larger than decomp buffer size (32 KB)
  const size_t chunk_size = 8000;

  const char letters[] = "abcdefghijklmnopqrstuvwxyz";
  unsigned char *in_buf = static_cast<unsigned char *>(smalloc(in_size));

  // random filling of letters
  for (size_t i = 0; i < in_size; i++) {
    in_buf[i] = letters[rand() % 26];
  }

  std::string in_path;
  FILE *in_f = CreateTempFile(sandbox_path, 0600, "w+", &in_path);
  fwrite(in_buf, 1, in_size, in_f);
  fclose(in_f);
  zip::InputPath input(in_path, chunk_size);

  std::string out_path;
  FILE *out_f = CreateTempFile(sandbox_path, 0600, "w+", &out_path);
  fclose(out_f);

  cvmfs::PathSink out(out_path);

  // Compress the output
  const zip::StreamStates res = compressor->Compress(&input, &out);

  EXPECT_EQ(res, zip::kStreamEnd);

  std::string decompress_path;
  FILE *decompress_f =
                     CreateTempFile(sandbox_path, 0600, "w+", &decompress_path);
  fclose(decompress_f);


  // Check if the data is the same as the beginning
  zip::InputPath compress(out_path);
  cvmfs::PathSink decompress_out(decompress_path);
  EXPECT_EQ(decompressor->DecompressStream(&compress, &decompress_out),
                                                               zip::kStreamEnd);


  decompress_f = fopen(decompress_path.c_str(), "rb");
  // get file sizes; read decompressed file into buffer
  fseek(decompress_f, 0L, SEEK_END);
  const size_t decompress_size = ftell(decompress_f);

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

// Also tests Input_File and SinkFile because *Path uses it under the hood
TEST_F(T_Compressor, CompressionSinkPath2PathLarge) {
  UniquePtr<Compressor> compressor;
  UniquePtr<Decompressor> decompressor;
  compressor = zip::Compressor::Construct(zip::kZlib);
  decompressor = zip::Decompressor::Construct(zip::kZlib);

  const size_t in_size = 16384 * 3ul;  // larger than decomp buffer size (32 KB)
  const size_t chunk_size = 8000;

  const char letters[] = "abcdefghijklmnopqrstuvwxyz";
  unsigned char *in_buf = static_cast<unsigned char *>(smalloc(in_size));

  // random filling of letters
  for (size_t i = 0; i < in_size; i++) {
    in_buf[i] = letters[rand() % 26];
  }

  std::string in_path;
  FILE *in_f = CreateTempFile(sandbox_path, 0600, "w+", &in_path);
  fwrite(in_buf, 1, in_size, in_f);
  fclose(in_f);
  zip::InputPath input(in_path, chunk_size);

  std::string out_path;
  FILE *out_f = CreateTempFile(sandbox_path, 0600, "w+", &out_path);
  fclose(out_f);

  cvmfs::PathSink out(out_path);

  // Compress the output
  const zip::StreamStates res = compressor->Compress(&input, &out);

  EXPECT_EQ(res, zip::kStreamEnd);

  std::string decompress_path;
  FILE *decompress_f =
                     CreateTempFile(sandbox_path, 0600, "w+", &decompress_path);
  fclose(decompress_f);


  // Check if the data is the same as the beginning
  zip::InputPath compress(out_path);
  cvmfs::PathSink decompress_out(decompress_path);
  EXPECT_EQ(decompressor->DecompressStream(&compress, &decompress_out),
                                                               zip::kStreamEnd);


  decompress_f = fopen(decompress_path.c_str(), "rb");
  // get file sizes; read decompressed file into buffer
  fseek(decompress_f, 0L, SEEK_END);
  const size_t decompress_size = ftell(decompress_f);

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

TEST_F(T_Compressor, ZstdCompressionSinkPathNull2Mem) {
  UniquePtr<Compressor> compressor;
  UniquePtr<Decompressor> decompressor;
  compressor = zip::Compressor::Construct(zip::kZstd);
  decompressor = zip::Decompressor::Construct(zip::kZstd);

  const size_t in_size = 0;

  zip::InputPath in(GetEmptyFile());
  cvmfs::MemSink out(0);

  const zip::StreamStates res = compressor->Compress(&in, &out);

  ASSERT_EQ(res, zip::kStreamEnd);
  ASSERT_GT(out.pos(), 0U);

  shash::Any file_hash(shash::kSha1);
  shash::HashMem(out.data(), out.pos(), &file_hash);

  EXPECT_EQ(file_hash.ToString(),
            "fb2e51cbd24e286dd066bd419d77cd772967e384");

  // Decompress it, check if it's still the same
  zip::InputMem compress(out.data(), out.pos());
  cvmfs::MemSink decompress_out(0);
  EXPECT_EQ(decompressor->DecompressStream(&compress, &decompress_out),
                                                               zip::kStreamEnd);

  EXPECT_EQ(in_size, decompress_out.pos());
}

TEST_F(T_Compressor, CompressionSinkPathNull2Mem) {
  UniquePtr<Compressor> compressor;
  UniquePtr<Decompressor> decompressor;
  compressor = zip::Compressor::Construct(zip::kZlib);
  decompressor = zip::Decompressor::Construct(zip::kZlib);

  const size_t in_size = 0;

  zip::InputPath in(GetEmptyFile());
  cvmfs::MemSink out(0);

  const zip::StreamStates res = compressor->Compress(&in, &out);

  ASSERT_EQ(res, zip::kStreamEnd);
  ASSERT_GT(out.pos(), 0U);

  shash::Any file_hash(shash::kSha1);
  shash::HashMem(out.data(), out.pos(), &file_hash);

  EXPECT_EQ(file_hash.ToString(),
            "e8ec3d88b62ebf526e4e5a4ff6162a3aa48a6b78");

  // Decompress it, check if it's still the same
  zip::InputMem compress(out.data(), out.pos());
  cvmfs::MemSink decompress_out(0);
  EXPECT_EQ(decompressor->DecompressStream(&compress, &decompress_out),
                                                               zip::kStreamEnd);

  EXPECT_EQ(in_size, decompress_out.pos());
}

TEST_F(T_Compressor, EchoCompressionSinkMem2MemLarge) {
  UniquePtr<Compressor> compressor;
  compressor = zip::Compressor::Construct(zip::kNoCompression);

  // Compress the output
  const size_t in_size = 16384;
  const size_t chunk_size = 8000;

  const char letters[] = "abcdefghijklmnopqrstuvwxyz";
  unsigned char *input = static_cast<unsigned char *>(smalloc(in_size));

  // random filling of letters
  for (size_t i = 0; i < in_size; i++) {
    input[i] = letters[rand() % 26];
  }

  zip::InputMem in(input, in_size, chunk_size);
  cvmfs::MemSink out(in_size);

  const zip::StreamStates res = compressor->Compress(&in, &out);

  EXPECT_EQ(res, zip::kStreamEnd);

  // Check if decompressed content is equal to original one
  EXPECT_EQ(0, memcmp(out.data(), input, in_size));

  free(input);
}

TEST_F(T_Compressor, EchoDecompressionSinkMem2MemLarge) {
  UniquePtr<Decompressor> decompressor;
  decompressor = zip::Decompressor::Construct(zip::kNoCompression);

  // Compress the output
  const size_t in_size = 16384;
  const size_t chunk_size = 8000;

  const char letters[] = "abcdefghijklmnopqrstuvwxyz";
  unsigned char *input = static_cast<unsigned char *>(smalloc(in_size));

  // random filling of letters
  for (size_t i = 0; i < in_size; i++) {
    input[i] = letters[rand() % 26];
  }

  zip::InputMem in(input, in_size, chunk_size);
  cvmfs::MemSink out(in_size);

  const zip::StreamStates res = decompressor->DecompressStream(&in, &out);

  EXPECT_EQ(res, zip::kStreamEnd);

  // Check if decompressed content is equal to original one
  EXPECT_EQ(0, memcmp(out.data(), input, in_size));

  free(input);
}

// Also tests Input_File and SinkFile because *Path uses it under the hood
TEST_F(T_Compressor, EchoCompressionSinkPath2PathLarge) {
  UniquePtr<Compressor> compressor;
  compressor = zip::Compressor::Construct(zip::kNoCompression);
  const size_t in_size = 16384 * 3ul;  // larger than decomp buffer size (32 KB)
  const size_t chunk_size = 8000;

  const char letters[] = "abcdefghijklmnopqrstuvwxyz";
  unsigned char *in_buf = static_cast<unsigned char *>(smalloc(in_size));

  // random filling of letters
  for (size_t i = 0; i < in_size; i++) {
    in_buf[i] = letters[rand() % 26];
  }

  std::string in_path;
  FILE *in_f = CreateTempFile(sandbox_path, 0600, "w+", &in_path);
  fwrite(in_buf, 1, in_size, in_f);
  fclose(in_f);
  zip::InputPath input(in_path, chunk_size);

  std::string out_path;
  FILE *out_f = CreateTempFile(sandbox_path, 0600, "w+", &out_path);
  fclose(out_f);

  cvmfs::PathSink out(out_path);

  // Compress the output
  const zip::StreamStates res = compressor->Compress(&input, &out);

  EXPECT_EQ(res, zip::kStreamEnd);

  out_f = fopen(out_path.c_str(), "rb");

  // get file sizes; read decompressed file into buffer
  fseek(out_f, 0L, SEEK_END);
  const size_t out_size = ftell(out_f);

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

// Also tests Input_File and SinkFile because *Path uses it under the hood
TEST_F(T_Compressor, EchoDecompressionSinkPath2PathLarge) {
  UniquePtr<Compressor> compressor;
  UniquePtr<Decompressor> decompressor;
  decompressor = zip::Decompressor::Construct(zip::kNoCompression);
  const size_t in_size = 16384 * 3ul;  // larger than decomp buffer size (32 KB)
  const size_t chunk_size = 8000;

  const char letters[] = "abcdefghijklmnopqrstuvwxyz";
  unsigned char *in_buf = static_cast<unsigned char *>(smalloc(in_size));

  // random filling of letters
  for (size_t i = 0; i < in_size; i++) {
    in_buf[i] = letters[rand() % 26];
  }

  std::string in_path;
  FILE *in_f = CreateTempFile(sandbox_path, 0600, "w+", &in_path);
  fwrite(in_buf, 1, in_size, in_f);
  fclose(in_f);
  zip::InputPath input(in_path, chunk_size);

  std::string out_path;
  FILE *out_f = CreateTempFile(sandbox_path, 0600, "w+", &out_path);
  fclose(out_f);

  cvmfs::PathSink out(out_path);

  // Compress the output
  const zip::StreamStates res = decompressor->DecompressStream(&input, &out);

  EXPECT_EQ(res, zip::kStreamEnd);

  out_f = fopen(out_path.c_str(), "rb");

  // get file sizes; read decompressed file into buffer
  fseek(out_f, 0L, SEEK_END);
  const size_t out_size = ftell(out_f);

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

TEST_F(T_Compressor, ZstdCompressionNewBigEnough) {
  UniquePtr<zip::Compressor> compressor;
  UniquePtr<zip::Decompressor> decompressor;
  compressor = zip::Compressor::Construct(zip::kZstd);

  // Compress the output
  unsigned char *input = reinterpret_cast<unsigned char *>(ptr_test_string);
  zip::InputMem in_mem(input, size_input);
  cvmfs::MemSink out_mem;
  out_mem.Adopt(buf_size, 0, buf, false);

  const zip::StreamStates ret =
      compressor->CompressStream(&in_mem, &out_mem, true);

  ASSERT_EQ(ret, zip::kStreamEnd);
  ASSERT_GT(out_mem.pos(), 0U);

  // Decompress it, check if it's still the same
  decompressor = zip::Decompressor::Construct(zip::kZstd);
  zip::InputMem in(out_mem.data(), out_mem.pos());
  cvmfs::MemSink out(0);
  const zip::StreamStates res = decompressor->DecompressStream(&in, &out);
  EXPECT_EQ(res, zip::kStreamEnd);
  EXPECT_EQ(out.pos(), strlen(test_string) + 1);
  EXPECT_EQ(0, memcmp(out.data(), test_string, strlen(test_string) + 1));
}

TEST_F(T_Compressor, CompressionNewBigEnough) {
  UniquePtr<Compressor> compressor;
  UniquePtr<Decompressor> decompressor;
  compressor = zip::Compressor::Construct(zip::kZlib);

  // Compress the output
  unsigned char *input = reinterpret_cast<unsigned char *>(ptr_test_string);
  zip::InputMem in_mem(input, size_input);
  cvmfs::MemSink out_mem;
  out_mem.Adopt(buf_size, 0, buf, false);

  const zip::StreamStates ret =
      compressor->CompressStream(&in_mem, &out_mem, true);

  ASSERT_EQ(ret, zip::kStreamEnd);
  ASSERT_GT(out_mem.pos(), 0U);

  // Decompress it, check if it's still the same
  decompressor = zip::Decompressor::Construct(zip::kZlib);
  zip::InputMem in(out_mem.data(), out_mem.pos());
  cvmfs::MemSink out(0);
  const zip::StreamStates res = decompressor->DecompressStream(&in, &out);
  EXPECT_EQ(res, zip::kStreamEnd);
  EXPECT_EQ(out.pos(), strlen(test_string) + 1);
  EXPECT_EQ(0, memcmp(out.data(), test_string, strlen(test_string) + 1));
}

void T_Compressor::ExerciseCompressionLongNewOutbufTooSmall(zip::Algorithm alg) {
  UniquePtr<Compressor> compressor;
  UniquePtr<Decompressor> decompressor;
  compressor = zip::Compressor::Construct(alg);
  unsigned compress_pos = 0;
  unsigned rounds = 0;

  const size_t in_size = 16384 * 3ul;  // larger than decomp buffer size (32 KB)
  const size_t chunk_size = 8000;

  const char letters[] = "abcdefghijklmnopqrstuvwxyz";
  unsigned char *in_buf = static_cast<unsigned char *>(smalloc(in_size));

  // random filling of letters
  for (size_t i = 0; i < in_size; i++) {
    in_buf[i] = letters[rand() % 26];
  }
  unsigned char *compress_buf =
    new unsigned char[compressor->CompressUpperBound(in_size)];


  zip::InputMem in_mem(in_buf, in_size, chunk_size, false);
  cvmfs::MemSink out_mem;

  zip::StreamStates ret = zip::kStreamContinue;
  while (ret != zip::kStreamEnd) {
    // Compress the output in multiple stages
    out_mem.Adopt(buf_size, 0, buf, false);

    ret = compressor->CompressStream(&in_mem, &out_mem, true);



    memcpy(compress_buf + compress_pos, out_mem.data(), out_mem.pos());
    compress_pos += out_mem.pos();
    rounds++;
  }

  EXPECT_GT(rounds, 1U);
  EXPECT_GT(compress_pos, 0U);

  // Decompress it, check if it's still the same
  decompressor = zip::Decompressor::Construct(alg);
  zip::InputMem in(compress_buf, compress_pos);
  cvmfs::MemSink out(0);
  const zip::StreamStates res = decompressor->DecompressStream(&in, &out);
  EXPECT_EQ(res, zip::kStreamEnd);
  EXPECT_EQ(out.pos(), in_size);
  EXPECT_EQ(0, memcmp(out.data(), in_buf, in_size));

  delete[] compress_buf;
}

TEST_F(T_Compressor, EveryCompressionLongNewOutbufTooSmall) {
  ExerciseCompressionLongNewOutbufTooSmall(zip::Algorithm::kNoCompression);
  ExerciseCompressionLongNewOutbufTooSmall(zip::Algorithm::kZlib);
  ExerciseCompressionLongNewOutbufTooSmall(zip::Algorithm::kZstd);
}

TEST_F(T_Compressor, CompressionLongNewOutbufTooSmall) {
  UniquePtr<Compressor> compressor;
  UniquePtr<Decompressor> decompressor;
  compressor = zip::Compressor::Construct(zip::kZlib);
  unsigned compress_pos = 0;
  unsigned rounds = 0;

  const size_t in_size = 16384 * 3ul;  // larger than decomp buffer size (32 KB)
  const size_t chunk_size = 8000;

  const char letters[] = "abcdefghijklmnopqrstuvwxyz";
  unsigned char *in_buf = static_cast<unsigned char *>(smalloc(in_size));

  // random filling of letters
  for (size_t i = 0; i < in_size; i++) {
    in_buf[i] = letters[rand() % 26];
  }
  unsigned char *compress_buf =
    new unsigned char[compressor->CompressUpperBound(in_size)];


  zip::InputMem in_mem(in_buf, in_size, chunk_size, false);
  cvmfs::MemSink out_mem;

  zip::StreamStates ret = zip::kStreamContinue;
  while (ret != zip::kStreamEnd) {
    // Compress the output in multiple stages
    out_mem.Adopt(buf_size, 0, buf, false);

    ret = compressor->CompressStream(&in_mem, &out_mem, true);

    memcpy(compress_buf + compress_pos, out_mem.data(), out_mem.pos());
    compress_pos += out_mem.pos();
    rounds++;
  }

  EXPECT_GT(rounds, 1U);
  EXPECT_GT(compress_pos, 0U);

  // Decompress it, check if it's still the same
  decompressor = zip::Decompressor::Construct(zip::kZlib);
  zip::InputMem in(compress_buf, compress_pos);
  cvmfs::MemSink out(0);
  const zip::StreamStates res = decompressor->DecompressStream(&in, &out);
  EXPECT_EQ(res, zip::kStreamEnd);
  EXPECT_EQ(out.pos(), in_size);
  EXPECT_EQ(0, memcmp(out.data(), in_buf, in_size));

  delete[] compress_buf;
}

TEST_F(T_Compressor, ZstdCompressionLongNewOutbufTooSmallMultiInput) {
  UniquePtr<Compressor> compressor;
  UniquePtr<Decompressor> decompressor;
  compressor = zip::Compressor::Construct(zip::kZstd);
  unsigned compress_pos = 0;
  unsigned rounds = 0;

  // for in_size: must be larger than internal buffer to force write to output.
  // Suggested block size is 65k by zstd
  const size_t in_size = 16384 * 9ul;
  const size_t in_size2 = 16384 * 9ul;  // larger than decomp buffer size
  const size_t chunk_size = 8000;

  const char letters[] = "abcdefghijklmnopqrstuvwxyz";
  unsigned char *in_buf = static_cast<unsigned char *>(smalloc(in_size));
  unsigned char *in_buf2 = static_cast<unsigned char *>(smalloc(in_size2));

  // random filling of letters
  for (size_t i = 0; i < in_size; i++) {
    in_buf[i] = letters[rand() % 26];
  }
  for (size_t i = 0; i < in_size2; i++) {
    in_buf2[i] = letters[rand() % 24];
  }

  const size_t total_max_bound = compressor->CompressUpperBound(in_size)
                                + compressor->CompressUpperBound(in_size2);
  unsigned char *compress_buf = new unsigned char[total_max_bound];


  zip::InputMem in_mem(in_buf, in_size, chunk_size, false);
  cvmfs::MemSink out_mem;


  zip::StreamStates ret = zip::kStreamError;
  while (ret != zip::kStreamEnd) {
    // Compress the output in multiple stages
    out_mem.Adopt(buf_size, 0, buf, false);

    ret = compressor->CompressStream(&in_mem, &out_mem, false);

    memcpy(compress_buf + compress_pos, out_mem.data(), out_mem.pos());
    compress_pos += out_mem.pos();
    rounds++;
  }

  EXPECT_GT(rounds, 1U);
  EXPECT_GT(compress_pos, 0U);

  const unsigned compress_pos_first = compress_pos;
  rounds = 0;
  zip::InputMem in_mem2(in_buf2, in_size2, chunk_size, false);
  cvmfs::MemSink out_mem2;

  ret = zip::kStreamError;
  while (ret != zip::kStreamEnd) {
    // Compress the output in multiple stages
    out_mem2.Adopt(buf_size, 0, buf, false);

    ret = compressor->CompressStream(&in_mem2, &out_mem2, true);

    memcpy(compress_buf + compress_pos, out_mem2.data(), out_mem2.pos());
    compress_pos += out_mem2.pos();
    rounds++;
  }

  EXPECT_GT(rounds, 1U);
  EXPECT_GT(compress_pos, compress_pos_first);

  // Decompress it, check if it's still the same
  decompressor = zip::Decompressor::Construct(zip::kZstd);
  zip::InputMem in(compress_buf, compress_pos);
  cvmfs::MemSink out(0);
  const zip::StreamStates res = decompressor->DecompressStream(&in, &out);

  EXPECT_EQ(res, zip::kStreamEnd);
  EXPECT_EQ(out.pos(), in_size + in_size2);
  EXPECT_EQ(0, memcmp(out.data(), in_buf, in_size));
  EXPECT_EQ(0, memcmp(out.data() + in_size, in_buf2, in_size2));

  delete[] compress_buf;
}

TEST_F(T_Compressor, CompressionLongNewOutbufTooSmallMultiInput) {
  UniquePtr<Compressor> compressor;
  UniquePtr<Decompressor> decompressor;
  compressor = zip::Compressor::Construct(zip::kZlib);
  unsigned compress_pos = 0;
  unsigned rounds = 0;

  const size_t in_size = 16384 * 3ul;  // larger than decomp buffer size (32 KB)
  const size_t in_size2 = 16384 * 2ul;  // larger than decomp buffer size
  const size_t chunk_size = 8000;

  const char letters[] = "abcdefghijklmnopqrstuvwxyz";
  unsigned char *in_buf = static_cast<unsigned char *>(smalloc(in_size));
  unsigned char *in_buf2 = static_cast<unsigned char *>(smalloc(in_size2));

  // random filling of letters
  for (size_t i = 0; i < in_size; i++) {
    in_buf[i] = letters[rand() % 26];
  }
  for (size_t i = 0; i < in_size2; i++) {
    in_buf2[i] = letters[rand() % 24];
  }

  const size_t total_max_bound = compressor->CompressUpperBound(in_size)
                                + compressor->CompressUpperBound(in_size2);
  unsigned char *compress_buf = new unsigned char[total_max_bound];


  zip::InputMem in_mem(in_buf, in_size, chunk_size, false);
  cvmfs::MemSink out_mem;

  zip::StreamStates ret = zip::kStreamError;
  while (ret != zip::kStreamEnd) {
    // Compress the output in multiple stages
    out_mem.Adopt(buf_size, 0, buf, false);

    ret = compressor->CompressStream(&in_mem, &out_mem, false);

    memcpy(compress_buf + compress_pos, out_mem.data(), out_mem.pos());
    compress_pos += out_mem.pos();
    rounds++;
  }

  EXPECT_GT(rounds, 1U);
  EXPECT_GT(compress_pos, 0U);

  const unsigned compress_pos_first = compress_pos;
  rounds = 0;
  zip::InputMem in_mem2(in_buf2, in_size2, chunk_size, false);
  cvmfs::MemSink out_mem2;

  ret = zip::kStreamError;
  while (ret != zip::kStreamEnd) {
    // Compress the output in multiple stages
    out_mem2.Adopt(buf_size, 0, buf, false);

    ret = compressor->CompressStream(&in_mem2, &out_mem2, true);

    memcpy(compress_buf + compress_pos, out_mem2.data(), out_mem2.pos());
    compress_pos += out_mem2.pos();
    rounds++;
  }

  EXPECT_GT(rounds, 1U);
  EXPECT_GT(compress_pos, compress_pos_first);

  // Decompress it, check if it's still the same
  decompressor = zip::Decompressor::Construct(zip::kZlib);
  zip::InputMem in(compress_buf, compress_pos);
  cvmfs::MemSink out(0);
  const zip::StreamStates res = decompressor->DecompressStream(&in, &out);

  EXPECT_EQ(res, zip::kStreamEnd);
  EXPECT_EQ(out.pos(), in_size + in_size2);
  EXPECT_EQ(0, memcmp(out.data(), in_buf, in_size));
  EXPECT_EQ(0, memcmp(out.data() + in_size, in_buf2, in_size2));

  delete[] compress_buf;
}

TEST_F(T_Compressor, ZstdCompressionLongNew) {
  UniquePtr<Compressor> compressor;
  UniquePtr<Decompressor> decompressor;
  compressor = zip::Compressor::Construct(zip::kZstd);

  unsigned char *compress_buf =
    new unsigned char[compressor->CompressUpperBound(long_size)];
  unsigned compress_pos = 0;;
  unsigned char *input = long_string;
  unsigned rounds = 0;
  const size_t chunk_size = 8000;


  zip::InputMem in_mem(input, long_size, chunk_size, false);
  cvmfs::MemSink out_mem;

  zip::StreamStates ret = zip::kStreamContinue;
  while (ret != zip::kStreamEnd) {
    // Compress the output in multiple stages
    out_mem.Adopt(buf_size, 0, buf, false);

    ret = compressor->CompressStream(&in_mem, &out_mem, true);

    memcpy(compress_buf + compress_pos, out_mem.data(), out_mem.pos());
    compress_pos += out_mem.pos();
    rounds++;
  }

  EXPECT_GT(rounds, 1U);
  EXPECT_GT(compress_pos, 0U);

  // Decompress it, check if it's still the same
  decompressor = zip::Decompressor::Construct(zip::kZstd);
  zip::InputMem in(compress_buf, compress_pos);
  cvmfs::MemSink out(0);
  const zip::StreamStates res = decompressor->DecompressStream(&in, &out);
  EXPECT_EQ(res, zip::kStreamEnd);
  EXPECT_EQ(out.pos(), static_cast<uint64_t>(long_size));
  EXPECT_EQ(0, memcmp(out.data(), long_string, long_size));

  delete[] compress_buf;
}

TEST_F(T_Compressor, CompressionLongNew) {
  UniquePtr<Compressor> compressor;
  UniquePtr<Decompressor> decompressor;
  compressor = zip::Compressor::Construct(zip::kZlib);

  unsigned char *compress_buf =
    new unsigned char[compressor->CompressUpperBound(long_size)];
  unsigned compress_pos = 0;;
  unsigned char *input = long_string;
  unsigned rounds = 0;
  const size_t chunk_size = 8000;


  zip::InputMem in_mem(input, long_size, chunk_size, false);
  cvmfs::MemSink out_mem;

  zip::StreamStates ret = zip::kStreamContinue;
  while (ret != zip::kStreamEnd) {
    // Compress the output in multiple stages
    out_mem.Adopt(buf_size, 0, buf, false);

    ret = compressor->CompressStream(&in_mem, &out_mem, true);

    memcpy(compress_buf + compress_pos, out_mem.data(), out_mem.pos());
    compress_pos += out_mem.pos();
    rounds++;
  }

  EXPECT_GT(rounds, 1U);
  EXPECT_GT(compress_pos, 0U);

  // Decompress it, check if it's still the same
  decompressor = zip::Decompressor::Construct(zip::kZlib);
  zip::InputMem in(compress_buf, compress_pos);
  cvmfs::MemSink out(0);
  const zip::StreamStates res = decompressor->DecompressStream(&in, &out);
  EXPECT_EQ(res, zip::kStreamEnd);
  EXPECT_EQ(out.pos(), static_cast<uint64_t>(long_size));
  EXPECT_EQ(0, memcmp(out.data(), long_string, long_size));

  delete[] compress_buf;
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

  UniquePtr<Compressor> compressor(Compressor::Construct(kZlibDefault));
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
