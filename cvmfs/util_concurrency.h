/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_CONCURRENCY_H_
#define CVMFS_UTIL_CONCURRENCY_H_

#include <pthread.h>

#include <queue>
#include <vector>
#include <set>

#include <cassert>

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

template <typename ParamT>
class CallbackBase {
 public:
  virtual void operator()(ParamT &value) = 0;
};

template <typename ParamT>
class Callback : public CallbackBase<ParamT> {
 public:
  typedef void (*CallbackFunction)(ParamT &value);

  Callback(CallbackFunction function) : function_(function) {}
  void operator()(ParamT &value) { function_(value); }

 private:
  CallbackFunction function_;
};

template <class DelegateT, typename ParamT>
class BoundCallback : public CallbackBase {
 public:
  typedef void (DelegateT::*CallbackMethod)(ParamT &value);

  BoundCallback(DelegateT* delegate, CallbackMethod method) :
    delegate_(delegate),
    method_(method) {}
  void operator()(ParamT &value) { (delegate_->*method_)(value); }

 private:
  DelegateT*     delegate_;
  CallbackMethod method_;
};

template <class WorkerT>
class ConcurrentWorkers {
 public:
  typedef typename WorkerT::expected_data  expected_data_t;
  typedef typename WorkerT::returned_data  returned_data_t;
  typedef typename WorkerT::worker_context worker_context_t;

 protected:
  typedef CallbackBase<returned_data_t>    callback_t;
  typedef std::set<callback_t>             Callbacks;
  typedef std::vector<pthread_t>           WorkerThreads;
  typedef std::queue<expected_data_t>      JobQueue;

  struct RunBinding {
    ConcurrentWorkers<WorkerT> *delegate;
    worker_context_t           *worker_context;
  }

 public:
  ConcurrentWorkers(const size_t      number_of_workers,
                    const size_t      maximal_queue_length,
                    worker_context_t *worker_context = NULL); // does NOT take ownership
  virtual ~ConcurrentWorkers();

  bool Initialize();

  void Schedule(const expected_data_t &data);
  void Terminate();

  void RegisterListener(callback_t *callback_object);    // takes ownership
  void UnregisterListener(callback_t *callback_object);
  void UnregisterListeners();

  void WaitForEmptyQueue() const;
  void WaitForTermination() const;

  inline unsigned int GetNumberOfWorkers() const { return number_of_workers_; }

 protected:
  bool SpawnWorkers();
  static void* RunWorker(void *run_binding); // takes ownership

  friend class WorkerT;
  expected_data_t Acquire();
  void JobDone(const returned_data_t& data, const bool success = true);
  inline void JobSuccessful(const returned_data_t& data) { JobDone(data, true); }
  inline void JobFailed(const returned_data_t& data) { JobDone(data, false); }

  inline bool is_running() const { return running_; }

 private:
  ConcurrentWorkers(const ConcurrentWorkers& other) { assert(false); } // do not copy!

 private:
  // general configuration
  const size_t                 number_of_workers_;
  const size_t                 maximal_queue_length_;
  const size_t                 desired_free_slots_;
  const worker_context_t      *worker_context_;

  // status information
  bool                         initialized_;
  bool                         running_;     // guarded by job_queue_mutex_

  // worker threads
  WorkerThreads                worker_threads_;

  // job queue
  JobQueue                     job_queue_;
  mutable pthread_mutex_t      job_queue_mutex_;
  mutable pthread_cond_t       job_queue_cond_not_empty_;
  mutable pthread_cond_t       job_queue_cond_not_full_;
  mutable pthread_cond_t       jobs_all_done_;
  mutable atomic_int32         jobs_pending_;
  mutable atomic_int32         jobs_failed_;

  // callback sub-system
  Callbacks                    listeners_;
  mutable pthread_mutex_t      listeners_mutex;
};

#ifdef CVMFS_NAMESPACE_GUARD
}
#endif

#include "util_concurrency_impl.h"

#endif /* CVMFS_UTIL_CONCURRENCY_H_ */
