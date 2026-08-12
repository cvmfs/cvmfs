/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FENCE_H_
#define CVMFS_FENCE_H_

#include <atomic>

#include "duplex_testing.h"
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
    counter_.store(0);
    blocking_.store(0);
  }

  void Enter() {
    while (blocking_.load()) {
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
    while (counter_.load() > 0) {
      SafeSleepMs(kBusyWaitBackoffMs);
    }
  }

  void Open() { atomic_cas32(&blocking_, 1, 0); }

 private:
  static const unsigned kBusyWaitBackoffMs = 100;

  /**
   * Number of active critical regions.
   */
  std::atomic<int64_t> counter_;

  /**
   * A boolean that indicates if the fence is blocked.
   */
  std::atomic<int32_t> blocking_;
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
