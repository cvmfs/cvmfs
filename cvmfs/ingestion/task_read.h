/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_INGESTION_TASK_READ_H_
#define CVMFS_INGESTION_TASK_READ_H_

#include <stdint.h>

#include <atomic>

#include "ingestion/item.h"
#include "ingestion/task.h"
#include "util/posix.h"
#include "util/tube.h"

class ItemAllocator;

class TaskRead : public TubeConsumer<FileItem> {
 public:
  static const unsigned kThrottleInitMs = 50;
  static const unsigned kThrottleMaxMs = 500;
  static const unsigned kThrottleResetMs = 2000;
  static const unsigned kBlockSize = kPageSize * 4;

  TaskRead(Tube<FileItem> *tube_in,
           TubeGroup<BlockItem> *tubes_out,
           ItemAllocator *allocator)
      : TubeConsumer<FileItem>(tube_in)
      , tubes_out_(tubes_out)
      , allocator_(allocator)
      , low_watermark_(0)
      , high_watermark_(0) {
    n_block_.store(0);
  }

  void SetWatermarks(uint64_t low, uint64_t high);

  uint64_t n_block() { return n_block_.load(); }

 protected:
  virtual void Process(FileItem *item);

 private:
  /**
   * Every new file increases the tag sequence counter that is used to annotate
   * BlockItems.
   */
  static std::atomic<int64_t> tag_seq_;

  TubeGroup<BlockItem> *tubes_out_;
  ItemAllocator *allocator_;
  /**
   * Continue reading once the amount of BlockItem managed bytes is back to
   * the given level.
   */
  uint64_t low_watermark_;
  /**
   * Stop reading new data into the pipeline when the amount BlockItem managed
   * byte is higher than the given level.
   */
  uint64_t high_watermark_;
  /**
   * Number of times reading was blocked on a high watermark.
   */
  std::atomic<int64_t> n_block_;
};

#endif  // CVMFS_INGESTION_TASK_READ_H_
