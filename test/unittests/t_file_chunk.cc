/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <vector>

#include "../../cvmfs/file_chunk.h"

using namespace std;  // NOLINT

class T_FileChunk : public ::testing::Test {
 protected:
  virtual void SetUp() {
  }
  virtual void TearDown() {
  }

  FileChunkReflist NewChunks() {
    FileChunkReflist result;
    result.list = new FileChunkList();
    result.path.Assign("/42", 3);
    return result;
  }

  SimpleChunkTables simple_;
};


TEST_F(T_FileChunk, FindChunkIdx) {
  FileChunkList single;
  FileChunkReflist reflist(&single, PathString(""), zlib::kZlibDefault, false);
  EXPECT_DEATH(reflist.FindChunkIdx(0), ".*");

  single.PushBack(FileChunk());
  EXPECT_EQ(0U, reflist.FindChunkIdx(0));
  EXPECT_EQ(0U, reflist.FindChunkIdx(100));

  single.PushBack(FileChunk(shash::Any(), 1, 0));
  EXPECT_EQ(0U, reflist.FindChunkIdx(0));
  EXPECT_EQ(1U, reflist.FindChunkIdx(1));
  EXPECT_EQ(1U, reflist.FindChunkIdx(100));

  for (unsigned i = 2; i < 10; ++i) {
    single.PushBack(FileChunk(shash::Any(), i, 0));
  }
  EXPECT_EQ(0U, reflist.FindChunkIdx(0));
  EXPECT_EQ(5U, reflist.FindChunkIdx(5));
  EXPECT_EQ(9U, reflist.FindChunkIdx(9));
  EXPECT_EQ(9U, reflist.FindChunkIdx(10));
  EXPECT_EQ(9U, reflist.FindChunkIdx(100));
}


TEST_F(T_FileChunk, Simple) {
  EXPECT_DEATH(simple_.Add(FileChunkReflist()), ".*");

  EXPECT_EQ(0, simple_.Add(NewChunks()));
  EXPECT_EQ(1, simple_.Add(NewChunks()));
  EXPECT_EQ(2, simple_.Add(NewChunks()));
  EXPECT_EQ(3, simple_.Add(NewChunks()));

  simple_.Release(-1);
  simple_.Release(4);

  simple_.Release(1);
  EXPECT_EQ(1, simple_.Add(NewChunks()));
  EXPECT_EQ(4, simple_.Add(NewChunks()));

  simple_.Release(3);
  simple_.Release(4);
  EXPECT_EQ(3, simple_.Add(NewChunks()));

  SimpleChunkTables::OpenChunks open_chunks;
  open_chunks = simple_.Get(0);
  EXPECT_TRUE(open_chunks.chunk_reflist.list != NULL);
  open_chunks = simple_.Get(-1);
  EXPECT_EQ(NULL, open_chunks.chunk_reflist.list);
  open_chunks = simple_.Get(4);
  EXPECT_EQ(NULL, open_chunks.chunk_reflist.list);

  simple_.Release(0);
  simple_.Release(1);
  simple_.Release(2);
  simple_.Release(3);
  EXPECT_EQ(0, simple_.Add(NewChunks()));
}
