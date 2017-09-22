/**
 * This file is part of the CernVM File System.
 */

 #include "ingestion/item.h"
 #include "ingestion/task.h"
 #include "util/posix.h"

class TaskCompression : public TubeConsumer<BlockItem> {
 public:
  static const unsigned kCompressedBlockSize = kPageSize * 2;

  TaskCompression(Tube<BlockItem> *tube_in, TubeGroup<BlockItem> *tubes_out)
    : TubeConsumer<BlockItem>(tube_in), tubes_out_(tubes_out) { }

 protected:
  virtual void Process(BlockItem *input_block);

 private:
  /**
   * Maps input block tag (hence: chunk) to the output block with the compressed
   * data.
   */
  typedef std::map<int64_t, BlockItem *> TagMap;

  TubeGroup<BlockItem> *tubes_out_;
  TagMap tag_map_;
};
