/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_INGESTION_ITEM_H_
#define CVMFS_INGESTION_ITEM_H_

#include <pthread.h>

#include <cassert>
#include <string>
#include <vector>

#include "util/single_copy.h"
#include "util_concurrency.h"

template <class ItemT>
class Tube;

class FileItem {
 public:
  explicit FileItem(const std::string &p) : path_(p) { }
  std::string path() { return path_; }

 private:
  std::string path_;
};


class BlockItem : SingleCopy {
 public:
  enum BlockType {
    kBlockHollow,
    kBlockData,
    kBlockStop,
  };

  BlockItem();
  ~BlockItem();
  void MakeStop();
  void MakeData(uint32_t capacity, BlockItem *succ_item);
  void MakeData(unsigned char *data, uint32_t size, BlockItem *succ_item);

  uint32_t Write(void *buf, uint32_t count);
  void Progress(Tube<BlockItem> *next_stage);
  void Progress(int32_t pred_nstage);
  void Discharge();

  unsigned char *data() {
    MutexLockGuard guard(&lock_);
    return data_;
  }

 private:
  void DoProgress();

  BlockType type_;

  unsigned char *data_;
  uint32_t capacity_;
  uint32_t size_;

  BlockItem *succ_item_;
  int32_t pred_nstage_;
  Tube<BlockItem> *next_stage_;

  pthread_mutex_t lock_;
};


#endif  // CVMFS_INGESTION_ITEM_H_
