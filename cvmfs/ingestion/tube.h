/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_INGESTION_TUBE_H_
#define CVMFS_INGESTION_TUBE_H_

#include <stdint.h>

#include <cassert>

#include "util/single_copy.h"
#include "util/pointer.h"

template <class ItemT>
class Tube : SingleCopy {
 public:
  class Link : SingleCopy {
    friend class Tube<ItemT>;
   public:
    explicit Link(ItemT *item) : item_(item), next_(NULL), prev_(NULL) { }
    ItemT *item() { return item_; }

   private:
    ItemT *item_;
    Link *next_;
    Link *prev_;
  };

  Tube() : limit_(0), size_(0) { Init(); }
  explicit Tube(uint64_t limit) : limit_(limit), size_(0) { Init(); }
  ~Tube() {
    Link *cursor = head_;
    do {
      Link *prev = cursor->prev_;
      delete cursor;
      cursor = prev;
    } while (cursor != head_);
  }

  bool IsEmpty() { return tail_->item() == NULL; }

  Link *Enqueue(ItemT *item) {
    assert(item != NULL);
    Link *link = new Link(item);
    link->next_ = tail_;
    link->prev_ = tail_->prev_;
    tail_->prev_->next_ = link;
    tail_->prev_ = link;
    tail_ = link;
    size_++;
    return link;
  }

  ItemT *Slice(Link *link) {
    link->prev_->next_ = link->next_;
    link->next_->prev_ = link->prev_;
    if (link == tail_)
      tail_ = head_;
    ItemT *item = link->item_;
    delete link;
    size_--;
    return item;
  }

  ItemT *Pop() {
    assert(!IsEmpty());
    return Slice(head_->prev_);
  }

  uint64_t size() { return size_; }

 private:
  void Init() {
    Link *sentinel = new Link(NULL);
    head_ = tail_ = sentinel;
    head_->next_ = head_->prev_ = sentinel;
    tail_->next_ = tail_->prev_ = sentinel;
  }

  uint64_t limit_;
  uint64_t size_;
  /**
   * In front of the first element (next in line for Pop())
   */
  Link *head_;
  /**
   * Points to the last inserted element
   */
  Link *tail_;
};

#endif  // CVMFS_INGESTION_TUBE_H_
