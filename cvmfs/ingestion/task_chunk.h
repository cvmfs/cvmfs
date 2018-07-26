/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_INGESTION_TASK_CHUNK_H_
#define CVMFS_INGESTION_TASK_CHUNK_H_

#include <stdint.h>

#include <map>

#include "atomic.h"
#include "ingestion/item.h"
#include "ingestion/task.h"
#include "ingestion/tube.h"
#include "util/posix.h"

class ItemAllocator;

class TaskChunk : public TubeConsumer<BlockItem> {
 public:
  TaskChunk(Tube<BlockItem> *tube_in,
            TubeGroup<BlockItem> *tubes_out,
            ItemAllocator *allocator)
    : TubeConsumer<BlockItem>(tube_in)
    , tubes_out_(tubes_out)
    , allocator_(allocator)
  { }

 protected:
  virtual void Process(BlockItem *input_block);

 private:
  /**
   * State of the chunk creation for a file
   */
  struct ChunkInfo {
    ChunkInfo()
      : offset(0)
      , output_tag_chunk(-1)
      , output_tag_bulk(-1)
      , next_chunk(NULL)
      , bulk_chunk(NULL)
    { }
    /**
     * Sum of input block size of the file that has been processed so far
     */
    uint64_t offset;
    /**
     * Blocks of the current regular chunk get tagged consistently
     */
    int64_t output_tag_chunk;
    /**
     * Blocks of the corresponding bulk chunk get a unique tag
     */
    int64_t output_tag_bulk;
    /**
     * The current regular chunk that corresponds to the output block stream;
     * may be NULL.
     */
    ChunkItem *next_chunk;
    /**
     * The whole file chunk attached to the file; may be NULL
     */
    ChunkItem *bulk_chunk;
  };

  /**
   * Maps input block tag (hence: file) to the state information on chunks.
   */
  typedef std::map<int64_t, ChunkInfo> TagMap;

  /**
   * Every new chunk increases the tag sequence counter that is used to annotate
   * BlockItems.
   */
  static atomic_int64 tag_seq_;

  TubeGroup<BlockItem> *tubes_out_;
  ItemAllocator *allocator_;
  TagMap tag_map_;
};

#endif  // CVMFS_INGESTION_TASK_CHUNK_H_
