/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_TUBE_H_
#define CVMFS_UTIL_TUBE_H_

#include <pthread.h>
#include <stdint.h>

#include <cassert>
#include <vector>

#include "util/atomic.h"
#include "util/concurrency.h"
#include "util/pointer.h"
#include "util/single_copy.h"

/**
 * A thread-safe, doubly linked list of links containing pointers to ItemT.  The
 * ItemT elements are not owned by the Tube.  FIFO or LIFO semantics.  Using
 * Slice(), items at arbitrary locations in the tube can be removed, too.
 *
 *
 * The layout of the linked list is as follows:
 *
 * --------------------------------------------------------------
 * |                                                            |
 * --> I$n$ (back) <--> I2 <--> ... <--> I1 (front) <--> HEAD <--
 *
 * The tube links the steps in the file processing pipeline.  It connects
 * multiple producers to multiple consumers and can throttle the producers if a
 * limit for the tube size is set.
 *
 * Internally, uses conditional variables to block when threads try to pop from
 * the empty tube or insert into the full tube.
 */
template<class ItemT>
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

  Tube() : limit_(uint64_t(-1)), size_(0) { Init(); }
  explicit Tube(uint64_t limit) : limit_(limit), size_(0) { Init(); }
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
  Link *EnqueueBack(ItemT *item) {
    assert(item != NULL);
    MutexLockGuard lock_guard(&lock_);
    while (size_ == limit_)
      pthread_cond_wait(&cond_capacious_, &lock_);

    Link *link = new Link(item);
    link->next_ = head_->next_;
    link->prev_ = head_;
    head_->next_->prev_ = link;
    head_->next_ = link;
    size_++;
    int retval = pthread_cond_signal(&cond_populated_);
    assert(retval == 0);
    return link;
  }

  /**
   * Push an item to the front of the queue. Block if queue currently full.
   */
  Link *EnqueueFront(ItemT *item) {
    assert(item != NULL);
    MutexLockGuard lock_guard(&lock_);
    while (size_ == limit_)
      pthread_cond_wait(&cond_capacious_, &lock_);

    Link *link = new Link(item);
    link->next_ = head_;
    link->prev_ = head_->prev_;
    head_->prev_->next_ = link;
    head_->prev_ = link;
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
  ItemT *PopFront() {
    MutexLockGuard lock_guard(&lock_);
    while (size_ == 0)
      pthread_cond_wait(&cond_populated_, &lock_);
    return SliceUnlocked(head_->prev_);
  }

  /**
   * Remove and return the first element from the queue if there is any.
   * Equivalent to an antomic
   *   ItemT item = NULL;
   *   if (!IsEmpty())
   *     item = PopFront();
   */
  ItemT *TryPopFront() {
    MutexLockGuard lock_guard(&lock_);
    // Note that we don't need to wait for a signal to arrive
    if (size_ == 0)
      return NULL;
    return SliceUnlocked(head_->prev_);
  }

  /**
   * Remove and return the last element from the queue.  Block if tube is
   * empty.
   */
  ItemT *PopBack() {
    MutexLockGuard lock_guard(&lock_);
    while (size_ == 0)
      pthread_cond_wait(&cond_populated_, &lock_);
    return SliceUnlocked(head_->next_);
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

 private:
  void Init() {
    Link *sentinel = new Link(NULL);
    head_ = sentinel;
    head_->next_ = head_->prev_ = sentinel;

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
    // Cannot delete the sentinel link
    assert(link != head_);
    link->prev_->next_ = link->next_;
    link->next_->prev_ = link->prev_;
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
   * Sentinel element in front of the first (front) element
   */
  Link *head_;
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


/**
 * A tube group manages a fixed set of Tubes and dispatches items among them in
 * such a way that items with the same tag (a positive integer) are all sent
 * to the same tube.
 */
template<class ItemT>
class TubeGroup : SingleCopy {
 public:
  TubeGroup() : is_active_(false) { atomic_init32(&round_robin_); }

  ~TubeGroup() {
    for (unsigned i = 0; i < tubes_.size(); ++i)
      delete tubes_[i];
  }

  void TakeTube(Tube<ItemT> *t) {
    assert(!is_active_);
    tubes_.push_back(t);
  }

  void Activate() {
    assert(!is_active_);
    assert(!tubes_.empty());
    is_active_ = true;
  }

  /**
   * Like Tube::EnqueueBack(), but pick a tube according to ItemT::tag()
   */
  typename Tube<ItemT>::Link *Dispatch(ItemT *item) {
    assert(is_active_);
    unsigned tube_idx = (tubes_.size() == 1) ? 0
                                             : (item->tag() % tubes_.size());
    return tubes_[tube_idx]->EnqueueBack(item);
  }

  /**
   * Like Tube::EnqueueBack(), use tubes one after another
   */
  typename Tube<ItemT>::Link *DispatchAny(ItemT *item) {
    assert(is_active_);
    unsigned tube_idx = (tubes_.size() == 1)
                            ? 0
                            : (atomic_xadd32(&round_robin_, 1) % tubes_.size());
    return tubes_[tube_idx]->EnqueueBack(item);
  }

 private:
  bool is_active_;
  std::vector<Tube<ItemT> *> tubes_;
  atomic_int32 round_robin_;
};

#endif  // CVMFS_UTIL_TUBE_H_
