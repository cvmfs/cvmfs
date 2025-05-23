/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_INGESTION_TASK_COMPRESS_H_
#define CVMFS_INGESTION_TASK_COMPRESS_H_

#include <map>

#include "ingestion/item.h"
#include "ingestion/task.h"
#include "smallhash.h"
#include "util/murmur.hxx"
#include "util/posix.h"

class ItemAllocator;

class TaskCompress : public TubeConsumer<BlockItem> {
 public:
  static const unsigned kCompressedBlockSize = kPageSize * 2;

  TaskCompress(Tube<BlockItem> *tube_in,
               TubeGroup<BlockItem> *tubes_out,
               ItemAllocator *allocator)
      : TubeConsumer<BlockItem>(tube_in)
      , tubes_out_(tubes_out)
      , allocator_(allocator) {
    tag_map_.Init(16, -1, hasher_int64t);
  }

 protected:
  virtual void Process(BlockItem *input_block);

 private:
  static inline uint32_t hasher_int64t(const int64_t &value) {
    return MurmurHash2(&value, sizeof(value), 0x07387a4f);
  }

  /**
   * Maps input block tag (hence: chunk) to the output block with the compressed
   * data.
   */
  typedef SmallHashDynamic<int64_t, BlockItem *> TagMap;

  TubeGroup<BlockItem> *tubes_out_;
  ItemAllocator *allocator_;
  TagMap tag_map_;
};

#endif  // CVMFS_INGESTION_TASK_COMPRESS_H_
