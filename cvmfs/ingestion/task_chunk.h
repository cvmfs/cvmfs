/**
 * This file is part of the CernVM File System.
 */

#include <stdint.h>

#include <map>

#include "atomic.h"
#include "ingestion/item.h"
#include "ingestion/task.h"
#include "ingestion/tube.h"
#include "util/posix.h"

class TaskChunk : public TubeConsumer<BlockItem> {
 public:
  TaskChunk(Tube<BlockItem> *tube_in, TubeGroup<BlockItem> *tubes_out)
    : TubeConsumer<BlockItem>(tube_in), tubes_out_(tubes_out) { }

 protected:
  virtual void Process(BlockItem *input_block);

 private:
  /**
   * State of a chunk under construction
   */
  struct ChunkInfo {
    ChunkInfo() : output_tag(-1), offset(-1) { }
    ChunkInfo(int64_t tag, int64_t off) : output_tag(tag), offset(off) { }
    int64_t output_tag;
    int64_t offset;
  };

  typedef std::map<int64_t, ChunkInfo> TagMap;

  /**
   * Every new chunk increases the tag sequence counter that is used to annotate
   * BlockItems.
   */
   static atomic_int64 tag_seq_;

  TubeGroup<BlockItem> *tubes_out_;
  TagMap tag_map_;
};
