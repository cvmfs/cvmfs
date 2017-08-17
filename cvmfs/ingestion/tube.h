/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_INGESTION_TUBE_H_
#define CVMFS_INGESTION_TUBE_H_

#include <pthread.h>
#include <stdint.h>

#include <cassert>

#include "util/single_copy.h"
#include "util/pointer.h"
#include "util_concurrency.h"

/**
 * A thread-safe, doubly linked list of links containing pointers to ItemT.  The
 * ItemT elements are not owned by the Tube.  FIFO semantics; items are pushed
 * to the back and poped from the front.  Using Slice(), items at arbitrary
 * locations in the tube can be removed, too.
 *
 * The tube links the steps in the file processing pipeline.  It connects
 * multiple producers to multiple consumers and can throttle the producers if a
 * limit for the tube size is set.
 *
 * Internally, uses conditional variables to block when threads try to pop from
 * the empty tube or insert into the full tube.
 */
template <class ItemT>
class Tube : SingleCopy {
 public:
  static const int32_t kMaxNstage = 4096;

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

  Tube() : limit_(uint64_t(-1)), size_(0), nstage_(0) { Init(); }
  explicit Tube(uint64_t limit) : limit_(limit), size_(0), nstage_(0) {
    Init();
  }
  ~Tube() {
    Link *cursor = head_;
    do {
      Link *prev = cursor->prev_;
      delete cursor;
      cursor = prev;
    } while (cursor != head_);
    pthread_cond_destroy(&cond_populated_);
    pthread_cond_destroy(&cond_capacious_);
    pthread_cond_destroy(&cond_empty_);
    pthread_mutex_destroy(&lock_);
  }

  /**
   * Push an item to the back of the queue.  Block if queue is currently full.
   */
  Link *Enqueue(ItemT *item) {
    assert(item != NULL);
    MutexLockGuard lock_guard(&lock_);
    while (size_ == limit_)
      pthread_cond_wait(&cond_capacious_, &lock_);

    Link *link = new Link(item);
    link->next_ = tail_;
    link->prev_ = tail_->prev_;
    tail_->prev_->next_ = link;
    tail_->prev_ = link;
    tail_ = link;
    size_++;
    int retval = pthread_cond_signal(&cond_populated_);
    assert(retval == 0);
    return link;
  }

  /**
   * Remove any link from the queue and return its item, including first/last
   * element.
   */
  ItemT *Slice(Link *link) {
    MutexLockGuard lock_guard(&lock_);
    return SliceUnlocked(link);
  }

  /**
   * Remove and return the first element from the queue.  Block if tube is
   * empty.
   */
  ItemT *Pop() {
    MutexLockGuard lock_guard(&lock_);
    while (size_ == 0)
      pthread_cond_wait(&cond_populated_, &lock_);
    return SliceUnlocked(head_->prev_);
  }

  /**
   * Blocks until the tube is empty
   */
  void Wait() {
    MutexLockGuard lock_guard(&lock_);
    while (size_ > 0)
      pthread_cond_wait(&cond_empty_, &lock_);
  }

  bool IsEmpty() {
    MutexLockGuard lock_guard(&lock_);
    return size_ == 0;
  }

  uint64_t size() {
    MutexLockGuard lock_guard(&lock_);
    return size_;
  }

  int32_t nstage() {
    MutexLockGuard lock_guard(&lock_);
    return nstage_;
  }

 private:
  void Init() {
    Link *sentinel = new Link(NULL);
    head_ = tail_ = sentinel;
    head_->next_ = head_->prev_ = sentinel;
    tail_->next_ = tail_->prev_ = sentinel;

    int retval = pthread_mutex_init(&lock_, NULL);
    assert(retval == 0);
    retval = pthread_cond_init(&cond_populated_, NULL);
    assert(retval == 0);
    retval = pthread_cond_init(&cond_capacious_, NULL);
    assert(retval == 0);
    retval = pthread_cond_init(&cond_empty_, NULL);
    assert(retval == 0);
  }

  ItemT *SliceUnlocked(Link *link) {
    link->prev_->next_ = link->next_;
    link->next_->prev_ = link->prev_;
    if (link == tail_)
      tail_ = head_;
    ItemT *item = link->item_;
    delete link;
    size_--;
    int retval = pthread_cond_signal(&cond_capacious_);
    assert(retval == 0);
    if (size_ == 0) {
      retval = pthread_cond_broadcast(&cond_empty_);
      assert(retval == 0);
    }
    return item;
  }


  /**
   * Adding new item blocks as long as limit_ == size_
   */
  uint64_t limit_;
  /**
   * The current number of links in the list
   */
  uint64_t size_;
  /**
   * If used in a pipline, stores the step number of this pipe
   */
  int32_t nstage_;
  /**
   * In front of the first element (next in line for Pop())
   */
  Link *head_;
  /**
   * Points to the last inserted element
   */
  Link *tail_;
  /**
   * Protects all internal state
   */
  pthread_mutex_t lock_;
  /**
   * Signals if there are items enqueued
   */
  pthread_cond_t cond_populated_;
  /**
   * Signals if there is space to enqueue more items
   */
  pthread_cond_t cond_capacious_;
  /**
   * Signals if the queue runs empty
   */
  pthread_cond_t cond_empty_;
};

#endif  // CVMFS_INGESTION_TUBE_H_
