#include <gtest/gtest.h>
#include <vector>

#include "../../cvmfs/file_processing/chunk_detector.h"
#include "../../cvmfs/file_processing/char_buffer.h"
#include "../../cvmfs/prng.h"

namespace upload {

class T_ChunkDetectors : public ::testing::Test {
 protected:
  void CreateBuffers(const size_t buffer_size) {
    ClearBuffers();

    const size_t MB          = 1048576;
    const size_t full_size   = 100 * MB;

    // make sure we always produce the same test data
    rng_.InitSeed(42);

    // produce some test data
    size_t i = 0;
    while (i < full_size) {
      CharBuffer * buffer = new CharBuffer(buffer_size);
      buffer->SetUsedBytes(std::min(full_size - i, buffer_size));
      buffer->SetBaseOffset(i);

      for (size_t j = 0; j < buffer->size(); ++j) {
        *(buffer->ptr() + j) = static_cast<unsigned char>(rng_.Next(256));
      }

      buffers_.push_back(buffer);
      i += buffer->used_bytes();
    }
  }

  virtual void TearDown() {
    ClearBuffers();
  }

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

TEST_F(T_ChunkDetectors, StaticOffsetChunkDetector) {
  const size_t static_chunk_size = 1024;

  StaticOffsetDetector static_offset_detector(static_chunk_size);
  EXPECT_FALSE (static_offset_detector.MightFindChunks(static_chunk_size));
  EXPECT_TRUE  (static_offset_detector.MightFindChunks(static_chunk_size + 1));

  CharBuffer buffer(static_chunk_size);
  buffer.SetUsedBytes(static_chunk_size / 2);

  off_t next_cut_mark = static_offset_detector.FindNextCutMark(&buffer);
  EXPECT_EQ (0, next_cut_mark);

  buffer.SetBaseOffset(buffer.used_bytes());
  next_cut_mark = static_offset_detector.FindNextCutMark(&buffer);
  EXPECT_EQ (0, next_cut_mark);

  buffer.SetBaseOffset(buffer.used_bytes() * 2);
  next_cut_mark = static_offset_detector.FindNextCutMark(&buffer);
  EXPECT_EQ (static_cast<off_t>(static_chunk_size), next_cut_mark);

  buffer.SetBaseOffset(buffer.used_bytes() * 3);
  next_cut_mark = static_offset_detector.FindNextCutMark(&buffer);
  EXPECT_EQ (0, next_cut_mark);

  CreateBuffers(1048576);

  off_t next_cut = 0;
  int   runs     = 2;
  Buffers::const_iterator i    = buffers_.begin();
  Buffers::const_iterator iend = buffers_.end();
  for (; i != iend; ++i) {
    while ((next_cut = static_offset_detector.FindNextCutMark(*i)) != 0) {
      EXPECT_EQ (static_cast<off_t>(static_chunk_size) * runs, next_cut);
      ++runs;
    }
  }
}


TEST_F(T_ChunkDetectors, Xor32) {
  Xor32Detector xor32_detector(1, 2, 4); // chunk sizes are not important here!

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
    EXPECT_EQ (values[i+1], xor32_detector.xor32_);
  }
}


TEST_F(T_ChunkDetectors, Xor32ChunkDetector) {
  const size_t base = 512000;
  const size_t min_chk_size = base;
  const size_t avg_chk_size = base * 2;
  const size_t max_chk_size = base * 4;
  Xor32Detector xor32_detector(min_chk_size,
                                       avg_chk_size,
                                       max_chk_size);

  EXPECT_FALSE (xor32_detector.MightFindChunks(0));
  EXPECT_FALSE (xor32_detector.MightFindChunks(base));
  EXPECT_TRUE  (xor32_detector.MightFindChunks(base + 1));
  EXPECT_TRUE  (xor32_detector.MightFindChunks(base * 2));
  EXPECT_TRUE  (xor32_detector.MightFindChunks(base * 3));
  EXPECT_TRUE  (xor32_detector.MightFindChunks(base * 3 + 1));

  // expected cut marks
  const off_t expected[] = {
      1478173,   3106048,   4774918,   5483479,   6145474,   6815524,   7993818,
      9827816,  10901280,  11472834,  12866669,  14914669,  15680095,  17728095,
     18270545,  20003471,  20608171,  21331281,  21978222,  22534719,  23082248,
     24152259,  25139243,  26454712,  28211455,  29378773,  30048887,  31106301,
     32584060,  33220713,  33782665,  34554410,  36509543,  37845212,  38357766,
     39026052,  39717365,  40902090,  41894611,  42565784,  43651833,  45340938,
     45957448,  46753480,  47668586,  48650727,  49349827,  50044561,  50609473,
     52448783,  53114708,  53757146,  54854971,  55503055,  56082722,  57245560,
     58088890,  59568818,  60827431,  62875431,  63539342,  64281928,  64832251,
     65513397,  67561397,  69265229,  70379107,  71426608,  72421269,  72983868,
     73937161,  74486883,  75507129,  76825236,  77691932,  79497499,  80381772,
     81515814,  82632302,  83187470,  83940560,  84919129,  85716196,  87018583,
     88055868,  90103868,  91215622,  93088799,  94465150,  95279802,  95895281,
     96736014,  97377339,  99274429, 100477630, 101018222, 101607802, 102485227,
    103100922, 103673232, 104338021
  };

  std::vector<size_t> buffer_sizes;
  buffer_sizes.push_back(102400);   // 100kB
  buffer_sizes.push_back(base);     // same as minimal chunk size
  buffer_sizes.push_back(base * 2); // same as average chunk size
  buffer_sizes.push_back(10485760); // 10MB

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

    for (; ! fail && j != jend; ++j) {
      while ((next_cut = detector.FindNextCutMark(*j)) != 0) {
        // check that the chunk size lies in the legal boundaries
        size_t chunk_size = next_cut - last_cut;
        if (max_chk_size < chunk_size) {
          EXPECT_GE (max_chk_size, chunk_size)
            << "too large chunk with buffer size " << *i << " bytes...";
          fail = true;
          break;
        }

        if (min_chk_size > chunk_size) {
          EXPECT_LE (min_chk_size, chunk_size)
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

} // namespace
