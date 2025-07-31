/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FENCE_H_
#define CVMFS_FENCE_H_

#include "duplex_testing.h"
#include "util/atomic.h"
#include "util/posix.h"
#include "util/single_copy.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

/**
 * A Fence can be used to protect critical regions where blocking is a very
 * rare operation.  When the Fence is not blocked, entering and leaving a
 * critical region requires only a 1-2 atomic operations.  In order to block
 * the fence, no new threads can enter a critical region.  When all entered
 * regions are left, the fence is blocked.  Waiting is done through slow busy
 * wait.
 */
class Fence : public SingleCopy {
  FRIEND_TEST(T_Fence, Basics);

 public:
  Fence() {
    atomic_init64(&counter_);
    atomic_init32(&blocking_);
  }

  void Enter() {
    while (atomic_read32(&blocking_)) {
      SafeSleepMs(kBusyWaitBackoffMs);
    }
    atomic_inc64(&counter_);
  }

  void Leave() { atomic_dec64(&counter_); }

  void Close() { atomic_cas32(&blocking_, 0, 1); }

  /**
   * Close and let live critical regions exit
   */
  void Drain() {
    Close();
    while (atomic_read64(&counter_) > 0) {
      SafeSleepMs(kBusyWaitBackoffMs);
    }
  }

  void Open() { atomic_cas32(&blocking_, 1, 0); }

 private:
  static const unsigned kBusyWaitBackoffMs = 100;

  /**
   * Number of active critical regions.
   */
  atomic_int64 counter_;

  /**
   * A boolean that indicates if the fence is blocked.
   */
  atomic_int32 blocking_;
};


/**
 * RAII wrapper in case an entire function or code block should be protected
 * by a fence.
 */
class FenceGuard {
 public:
  explicit FenceGuard(Fence *fence) : fence_(fence) { fence_->Enter(); }
  ~FenceGuard() { fence_->Leave(); }

 private:
  Fence *fence_;
};

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_FENCE_H_
