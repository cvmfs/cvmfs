/**
 * This file is part of the CernVM File System.
 */

#include "item.h"

#include <algorithm>
#include <cstring>

#include "ingestion/tube.h"
#include "smalloc.h"
#include "util_concurrency.h"

BlockItem::BlockItem()
  : type_(kBlockHollow)
  , data_(NULL)
  , capacity_(0)
  , size_(0)
  , succ_item_(NULL)
  , pred_nstage_(Tube<BlockItem>::kMaxNstage)
  , next_stage_(NULL)
{
  int retval = pthread_mutex_init(&lock_, NULL);
  assert(retval == 0);
}


BlockItem::~BlockItem() {
  free(data_);
  pthread_mutex_destroy(&lock_);
}


void BlockItem::Discharge() {
  if (succ_item_ != NULL)
    succ_item_->Progress(Tube<BlockItem>::kMaxNstage);
}


void BlockItem::DoProgress() {
  if (next_stage_ == NULL)
    return;
  int32_t next_nstage = next_stage_->nstage();
  if (next_nstage >= pred_nstage_)
    return;

  next_stage_->Enqueue(this);
  if (succ_item_ != NULL)
    succ_item_->Progress(next_nstage);
  next_stage_ = NULL;
}


void BlockItem::MakeStop() {
  MutexLockGuard guard(&lock_);
  assert(type_ == kBlockHollow);
  type_ = kBlockStop;
}


void BlockItem::MakeData(uint32_t capacity, BlockItem *succ_item) {
  MutexLockGuard guard(&lock_);
  assert(type_ == kBlockHollow);
  assert(succ_item != NULL);

  type_ = kBlockData;
  capacity_ = capacity_;
  data_ = reinterpret_cast<unsigned char *>(smalloc(capacity_));
  succ_item_ = succ_item;
  succ_item_->Progress(0);
}


/**
 * Move data from one block to another.
 */
void BlockItem::MakeData(
  unsigned char *data,
  uint32_t size,
  BlockItem *succ_item)
{
  MutexLockGuard guard(&lock_);
  assert(type_ == kBlockHollow);
  assert(succ_item != NULL);

  type_ = kBlockData;
  capacity_ = size_ = size;
  data_ = data;
  succ_item_ = succ_item;
  succ_item_->Progress(0);
}


void BlockItem::Progress(Tube<BlockItem> *next_stage) {
  MutexLockGuard guard(&lock_);
  assert(type_ != kBlockHollow);

  next_stage_ = next_stage;
  DoProgress();
}


void BlockItem::Progress(int32_t pred_nstage) {
  MutexLockGuard guard(&lock_);
  pred_nstage_ = pred_nstage;
  DoProgress();
}


uint32_t BlockItem::Write(void *buf, uint32_t count) {
  MutexLockGuard guard(&lock_);
  assert(type_ == kBlockData);

  uint32_t remaining = capacity_ - size_;
  uint32_t nbytes = std::min(remaining, count);
  memcpy(data_ + size_, buf, nbytes);
  size_ += nbytes;
  return nbytes;
}
