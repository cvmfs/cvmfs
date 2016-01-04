/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <vector>

#include "../../cvmfs/file_processing/char_buffer.h"
#include "../../cvmfs/file_processing/chunk_detector.h"
#include "../../cvmfs/prng.h"

namespace upload {

class T_ChunkDetectors : public ::testing::Test {
 protected:
  static const size_t data_size_ = 104857600;  // 100 MiB

  void CreateBuffers(const size_t buffer_size) {
    ClearBuffers();

    // make sure we always produce the same test data
    rng_.InitSeed(42);

    // produce some test data
    size_t i = 0;
    while (i < data_size()) {
      CharBuffer * buffer = new CharBuffer(buffer_size);
      buffer->SetUsedBytes(std::min(data_size() - i, buffer_size));
      buffer->SetBaseOffset(i);

      for (size_t j = 0; j < buffer->size(); ++j) {
        *(buffer->ptr() + j) = static_cast<unsigned char>(rng_.Next(256));
      }

      buffers_.push_back(buffer);
      i += buffer->used_bytes();
    }
  }

  void CreateZeroBuffers(const size_t buffer_size) {
    ClearBuffers();

    size_t i = 0;
    while (i < data_size()) {
      CharBuffer *buffer = new CharBuffer(buffer_size);
      const size_t bytes_to_write = std::min(data_size() - i, buffer_size);

      memset(buffer->free_space_ptr(), 0, bytes_to_write);
      buffer->SetUsedBytes(bytes_to_write);
      buffer->SetBaseOffset(i);

      buffers_.push_back(buffer);
      i += buffer->used_bytes();
    }
  }

  virtual void TearDown() {
    ClearBuffers();
  }

  size_t data_size() const { return data_size_; }

 private:
  void ClearBuffers() {
    Buffers::const_iterator i    = buffers_.begin();
    Buffers::const_iterator iend = buffers_.end();
    for (; i != iend; ++i) {
      delete *i;
    }
    buffers_.clear();
  }

 protected:
  typedef std::vector<CharBuffer*> Buffers;
  Buffers buffers_;

 private:
  Prng rng_;
};

TEST_F(T_ChunkDetectors, StaticOffsetChunkDetectorSlow) {
  const size_t static_chunk_size = 1024;

  StaticOffsetDetector static_offset_detector(static_chunk_size);
  EXPECT_FALSE(static_offset_detector.MightFindChunks(static_chunk_size));
  EXPECT_TRUE(static_offset_detector.MightFindChunks(static_chunk_size + 1));

  CharBuffer buffer(static_chunk_size);
  buffer.SetUsedBytes(static_chunk_size / 2);

  off_t next_cut_mark = static_offset_detector.FindNextCutMark(&buffer);
  EXPECT_EQ(0, next_cut_mark);

  buffer.SetBaseOffset(buffer.used_bytes());
  next_cut_mark = static_offset_detector.FindNextCutMark(&buffer);
  EXPECT_EQ(0, next_cut_mark);

  buffer.SetBaseOffset(buffer.used_bytes() * 2);
  next_cut_mark = static_offset_detector.FindNextCutMark(&buffer);
  EXPECT_EQ(static_cast<off_t>(static_chunk_size), next_cut_mark);

  buffer.SetBaseOffset(buffer.used_bytes() * 3);
  next_cut_mark = static_offset_detector.FindNextCutMark(&buffer);
  EXPECT_EQ(0, next_cut_mark);

  CreateBuffers(1048576);

  off_t next_cut = 0;
  int   runs     = 2;
  Buffers::const_iterator i    = buffers_.begin();
  Buffers::const_iterator iend = buffers_.end();
  for (; i != iend; ++i) {
    while ((next_cut = static_offset_detector.FindNextCutMark(*i)) != 0) {
      EXPECT_EQ(static_cast<off_t>(static_chunk_size) * runs, next_cut);
      ++runs;
    }
  }
}


TEST_F(T_ChunkDetectors, Xor32) {
  Xor32Detector xor32_detector(1, 2, 4);  // chunk sizes are not important here!

  // table of test data:
  //   <input value> , <expected xor32 value>
  const uint32_t value_count = 96;
  uint32_t values[value_count] = {
      5u,          5u,
    255u,        245u,
      0u,        490u,
     32u,       1012u,
     27u,       2035u,
     11u,       4077u,
     87u,       8077u,
    128u,      16282u,
    127u,      32587u,
    224u,      65142u,
     63u,     130259u,
     11u,     260525u,
      1u,     521051u,
    103u,    1042129u,
     73u,    2084331u,
     22u,    4168640u,
    235u,    8337259u,
     17u,   16674503u,
      3u,   33349005u,
     90u,   66698048u,
    210u,  133396050u,
    163u,  266791943u,
     12u,  533583874u,
     79u, 1067167819u,
     53u, 2134335651u,
      2u, 4268671300u,
    100u, 4242375404u,
    193u, 4189783321u,
     64u, 4084599410u,
    242u, 3874231318u,
     14u, 3453495330u,
    111u, 2612023339u,
     83u,  929079301u,
    253u, 1858158839u,
    207u, 3716317473u,
    172u, 3137667822u,
     62u, 1980368354u,
    190u, 3960736634u,
    114u, 3626505862u,
     39u, 2958044459u,
      6u, 1621121616u,
    175u, 3242243087u,
     93u, 2189518915u,
    238u,   84070504u,
    184u,  168140904u,
     84u,  336281732u,
     29u,  672563477u,
    200u, 1345127138u
  };

  for (unsigned int i = 0; i < value_count; i+=2) {
    xor32_detector.xor32(static_cast<unsigned char>(values[i]));
    EXPECT_EQ(values[i+1], xor32_detector.xor32_);
  }
}


TEST_F(T_ChunkDetectors, Xor32ChunkDetectorSlow) {
  const size_t base = 512000;
  const size_t min_chk_size = base;
  const size_t avg_chk_size = base * 2;
  const size_t max_chk_size = base * 4;
  Xor32Detector xor32_detector(min_chk_size,
                                       avg_chk_size,
                                       max_chk_size);

  EXPECT_FALSE(xor32_detector.MightFindChunks(0));
  EXPECT_FALSE(xor32_detector.MightFindChunks(base));
  EXPECT_TRUE(xor32_detector.MightFindChunks(base + 1));
  EXPECT_TRUE(xor32_detector.MightFindChunks(base * 2));
  EXPECT_TRUE(xor32_detector.MightFindChunks(base * 3));
  EXPECT_TRUE(xor32_detector.MightFindChunks(base * 3 + 1));

  // expected cut marks
  const off_t expected[] = {
      742752,   2790752,   3521796,    4188893,   4940543,   5884591,   7131564,
     8520617,   9159233,   9999783,   11202120,  11778185,  13781177,  15331152,
    16497018,  17410317,  19391774,   20086877,  20763223,  21346744,  22075173,
    22705444,  23557037,  24461261,   25118505,  25663669,  26333579,  27763062,
    28369273,  29586929,  30171732,   30794768,  31811364,  32408599,  33689064,
    34678262,  36410687,  37096663,   37682005,  38269900,  39931592,  41347607,
    42701845,  44350286,  44925654,   45893350,  47770173,  48332690,  49303250,
    49861260,  50727512,  51578678,   52176356,  52949949,  54228704,  54843051,
    55796214,  56694940,  58092222,   59294196,  60381155,  61509784,  62850141,
    63505051,  64168046,  64925423,   66252082,  67232078,  68005369,  68615410,
    69454686,  70276575,  71308716,   73356716,  73946267,  74775664,  75509293,
    77487075,  78606807,  79732133,   80467148,  81185709,  82588680,  83547234,
    84220971,  85992169,  86979928,   87845294,  88392648,  89241756,  89980293,
    91129752,  91891024,  92493793,   93792263,  94393106,  95782791,  96813116,
    97723756,  98618891,  99707336,  101755336, 103405168, 104269441
  };

  std::vector<size_t> buffer_sizes;
  buffer_sizes.push_back(102400);    // 100kB
  buffer_sizes.push_back(base);      // same as minimal chunk size
  buffer_sizes.push_back(base * 2);  // same as average chunk size
  buffer_sizes.push_back(10485760);  // 10MB

  std::vector<size_t>::const_iterator i    = buffer_sizes.begin();
  std::vector<size_t>::const_iterator iend = buffer_sizes.end();
  for (; i != iend; ++i) {
    // create the test data chunked into buffers of size *i
    CreateBuffers(*i);

    // go through all the buffers and check if the chunker produces the
    // expected results
    Xor32Detector detector(base, base * 2, base * 4);
    off_t next_cut = 0;
    off_t last_cut = 0;
    int   cut      = 0;
    bool  fail     = false;
    Buffers::const_iterator j    = buffers_.begin();
    Buffers::const_iterator jend = buffers_.end();

    for (; !fail && j != jend; ++j) {
      while ((next_cut = detector.FindNextCutMark(*j)) != 0) {
        // check that the chunk size lies in the legal boundaries
        size_t chunk_size = next_cut - last_cut;
        if (max_chk_size < chunk_size) {
          EXPECT_GE(max_chk_size, chunk_size)
            << "too large chunk with buffer size " << *i << " bytes...";
          fail = true;
          break;
        }

        if (min_chk_size > chunk_size) {
          EXPECT_LE(min_chk_size, chunk_size)
            << "too small chunk with buffer size " << *i << " bytes...";
          fail = true;
          break;
        }

        // check that chunk boundary is correct
        const int index = cut++;
        if (expected[index] != next_cut) {
          EXPECT_EQ(expected[index], next_cut)
            << "unexpected cut mark with buffer size " << *i << " bytes...";
          fail = true;
          break;
        }

        last_cut = next_cut;
      }
    }
  }
}


TEST_F(T_ChunkDetectors, Xor32ChunkDetectorZerosBufferPowerOfTwo) {
  // This is a regression test for CVM-957, describing a bug in the XOR 32 chunk
  // detector that crashes with certain input. Namely, if the provided data does
  // not contain any XOR32 cutmarks (i.e. it is cut at 'max chunk size') and the
  // number of bytes are an exact multiple of 'max chunk size'.

  ASSERT_EQ(0u, data_size() % 16);
  ASSERT_EQ(0u, data_size() % 32);
  ASSERT_EQ(0u, data_size() % 64);

  const size_t min_chk_size = data_size() / 64;
  const size_t avg_chk_size = data_size() / 32;
  const size_t max_chk_size = data_size() / 16;
  Xor32Detector xor32_detector(min_chk_size,
                               avg_chk_size,
                               max_chk_size);

  CreateZeroBuffers(512000);

  off_t next_cut = 0;
  bool  fail     = false;
  Buffers::const_iterator j    = buffers_.begin();
  Buffers::const_iterator jend = buffers_.end();
  for (; !fail && j != jend; ++j) {
    while ((next_cut = xor32_detector.FindNextCutMark(*j)) != 0) {
      ASSERT_LE(0u, next_cut);
      EXPECT_EQ(0u, next_cut % max_chk_size);
      // ChunkDetector might decide to cut right in the end of a file. This is
      // because it works on CharBuffer-level and doesn't have a notion about
      // the actual file's size.  Hence: EXPECT_GreaterEqual()
      EXPECT_GE(data_size(), static_cast<size_t>(next_cut));
    }
  }
}

}  // namespace upload
