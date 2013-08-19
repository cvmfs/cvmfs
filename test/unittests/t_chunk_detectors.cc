#include <gtest/gtest.h>
#include <cstdlib>
#include <vector>

#include <iostream> // TODO: remove me

#include "../../cvmfs/upload_file_processing/chunk_detector.h"
#include "../../cvmfs/upload_file_processing/char_buffer.h"

class T_ChunkDetectors : public ::testing::Test {
 protected:
  void CreateBuffers(const size_t buffer_size) {
    ClearBuffers();

    const size_t MB          = 1048576;
    const size_t full_size   = 100 * MB;

    // make sure we always produce the same test data
    srand(42);

    // produce some test data
    size_t i = 0;
    while (i < full_size) {
      upload::CharBuffer * buffer = new upload::CharBuffer(buffer_size);
      buffer->SetUsedBytes(std::min(full_size - i, buffer_size));
      buffer->SetBaseOffset(i);

      for (size_t j = 0; j < buffer->size(); ++j) {
        *(buffer->ptr() + j) = rand() % 256;
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
  typedef std::vector<upload::CharBuffer*> Buffers;
  Buffers buffers_;
};

TEST_F(T_ChunkDetectors, StaticOffsetChunkDetector) {
  const size_t static_chunk_size = 1024;

  upload::StaticOffsetDetector static_offset_detector(static_chunk_size);
  EXPECT_FALSE (static_offset_detector.MightFindChunks(static_chunk_size));
  EXPECT_TRUE  (static_offset_detector.MightFindChunks(static_chunk_size + 1));

  upload::CharBuffer buffer(static_chunk_size);
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


TEST_F(T_ChunkDetectors, Xor32ChunkDetector) {
  const size_t base = 512000;
  upload::Xor32Detector xor32_detector(base, base * 2, base * 4);

  EXPECT_FALSE (xor32_detector.MightFindChunks(0));
  EXPECT_FALSE (xor32_detector.MightFindChunks(base));
  EXPECT_TRUE  (xor32_detector.MightFindChunks(base + 1));
  EXPECT_TRUE  (xor32_detector.MightFindChunks(base * 2));
  EXPECT_TRUE  (xor32_detector.MightFindChunks(base * 3));
  EXPECT_TRUE  (xor32_detector.MightFindChunks(base * 3 + 1));

  // expected cut marks
  const off_t expected[] = {
      591544,  1112589,  1716139,   2677179,   3473340,   4075967,   4832346,
     5467359,  6584843,  7233161,   8237075,   8911472,  10211539,  11708173,
    12545542, 13150940, 13746430,  14902448,  15841437,  16458881,  17123801,
    17959953, 19894517, 21942517,  22888955,  23820032,  24355174,  25084484,
    25791530, 27398981, 28841447,  29685225,  30533040,  32581040,  33565322,
    34846885, 35378061, 36354275,  37469200,  38049328,  38748025,  40526310,
    41244625, 41925007, 42497297,  43830589,  44347080,  44978978,  45843784,
    46489702, 47049471, 47713371,  48414503,  50118479,  50834131,  52830209,
    53505714, 54385890, 55416918,  56736198,  57516639,  58112403,  58705576,
    59540049, 60904495, 61734883,  62667100,  63296723,  63908168,  64952264,
    66536570, 67562340, 68734626,  69540127,  71148388,  71905512,  72813152,
    73372989, 74072441, 74621306,  75181178,  75996775,  76631479,  77206009,
    77724169, 79104681, 79710589,  81014713,  81612478,  82360921,  83350791,
    84079423, 84809053, 85439794,  86132602,  87432725,  88277767,  88889874,
    90188927, 90949653, 91472336,  92216519,  92985916,  94199809,  95523530,
    96884562, 98506308, 99403409, 101084089, 101672598, 102271189, 103238158,
    104050039
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
    upload::Xor32Detector detector(base, base * 2, base * 4);
    off_t next_cut = 0;
    int   cut      = 0;
    bool fail      = false;
    Buffers::const_iterator j    = buffers_.begin();
    Buffers::const_iterator jend = buffers_.end();
    for (; ! fail && j != jend; ++j) {
      while ((next_cut = detector.FindNextCutMark(*j)) != 0) {
        const int index = cut++;
        if (expected[index] != next_cut) {
          EXPECT_EQ(expected[index], next_cut) << "failed with buffer size "
                                               << *i << " byte... ";
          fail = true;
        }
      }
    }
  }
}
