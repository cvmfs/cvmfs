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

#include "atomic.h"
#include "util.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

/**
 * RAII ftw!
 * This is a very simple scooped lock implementation. Every object that has the
 * methods Lock() and Unlock() should work with it.
 * Creating a LockGuard object on the stack will lock the provided object. When
 * the LockGuard runs out of scope it will automatically release the lock. This
 * ensures a clean unlock in a lot of situations!
 */
template<typename T>
class LockGuard {
 public:
  LockGuard(T &lock) :
    ref_(lock)
  {
    ref_.Lock();
  }

  LockGuard(T *lock) :
    ref_(*lock)
  {
    ref_.Lock();
  }

  ~LockGuard() {
    ref_.Unlock();
  }

 private:
  LockGuard(const LockGuard&); // don't copy that!

  T &ref_;
};

/**
 * Template specialization to enable the scooped lock described above to use
 * plain POSIX mutexes
 */
template<>
class LockGuard <pthread_mutex_t> {
 public:
  LockGuard(pthread_mutex_t &lock) :
    ref_(lock)
  {
    pthread_mutex_lock(&ref_); 
  }

  ~LockGuard() {
    pthread_mutex_unlock(&ref_);
  }

  pthread_mutex_t &ref_;
};
typedef LockGuard<pthread_mutex_t> MutexLockGuard;

/**
 * TODO: Find a better way than copy/paste for this 'multipe template specialzation'.
 *       
 * This lock guards will acquire a write lock or a read lock!
 *                                (WriteLockGuard)(ReadLockGuard)
 */
class WriteLockGuard {
 public:
  WriteLockGuard(pthread_rwlock_t &lock) :
    ref_(lock)
  {
    pthread_rwlock_wrlock(&ref_);
  }

  ~WriteLockGuard() {
    pthread_rwlock_unlock(&ref_);
  }

  pthread_rwlock_t &ref_;
};
class ReadLockGuard {
 public:
  ReadLockGuard(pthread_rwlock_t &lock) :
    ref_(lock)
  {
    pthread_rwlock_rdlock(&ref_);
  }

  ~ReadLockGuard() {
    pthread_rwlock_unlock(&ref_);
  }

  pthread_rwlock_t &ref_;
};

//
// -----------------------------------------------------------------------------
//

/**
 * Encapsulates a callback function that handles asynchronous responses.
 *
 * This is an abstract base class for two different callback function objects.
 * There are two specializations:
 *  --> 1. for static members or global C-like functions
 *  --> 2. for member functions of arbitrary objects
 */
template <typename ParamT>
class CallbackBase {
 public:
  virtual void operator()(const ParamT &value) = 0;
  virtual ~CallbackBase() {}
};

/**
 * This callback function object can be used to call static members or global
 * functions with the following signature:
 * void <name>(ParamT <parameter>);
 *
 * TODO: One might use varidic templates once C++11 will be supported in order
 *       to allow for more than one parameter.
 *
 * @param ParamT    the type of the parameter to be passed to the callback
 */
template <typename ParamT>
class Callback : public CallbackBase<ParamT> {
 public:
  typedef void (*CallbackFunction)(const ParamT &value);

  Callback(CallbackFunction function) : function_(function) {}
  void operator()(const ParamT &value) { function_(value); }

 protected:
  Callback(const Callback& callback) { assert (false); } // don't copy!

 private:
  CallbackFunction function_;
};

/**
 * A BoundCallback can be used to call a member of an arbitrary object as a
 * callback.
 * The member must have the following interface:
 * void <DelegateT>::<member name>(ParamT <parameter>);
 *
 * @param DelegateT   the <class name> of the object the member <member name>
 *                    should be invoked in
 * @param ParamT      the type of the parameter to be passed to the callback
 */
template <typename ParamT, class DelegateT>
class BoundCallback : public CallbackBase<ParamT> {
 public:
  typedef void (DelegateT::*CallbackMethod)(const ParamT &value);

  BoundCallback(CallbackMethod method, DelegateT *delegate) :
    delegate_(delegate),
    method_(method) {}
  BoundCallback(CallbackMethod method, DelegateT &delegate) :
    delegate_(&delegate),
    method_(method) {}

  void operator()(const ParamT &value) { (delegate_->*method_)(value); }

 protected:
  BoundCallback(const BoundCallback& callback) { assert (false); } // don't copy!

 private:
  DelegateT*     delegate_;
  CallbackMethod method_;
};

template <typename ParamT>
class Observable {
 public:
  typedef CallbackBase<ParamT> *callback_t;

 protected:
  typedef std::set<callback_t> Callbacks;

 public:
  Observable();
  virtual ~Observable();

  template <class DelegateT>
  callback_t RegisterListener(typename BoundCallback<ParamT, DelegateT>::CallbackMethod method,
                              DelegateT *delegate);
  callback_t RegisterListener(typename Callback<ParamT>::CallbackFunction fn);

  void UnregisterListener(callback_t callback_object);
  void UnregisterListeners();

 protected:
  void RegisterListener(callback_t callback_object);
  void NotifyListeners(const ParamT &parameter);

 private:
  Callbacks                    listeners_;
  mutable pthread_rwlock_t     listeners_rw_lock_;
};

//
// -----------------------------------------------------------------------------
//

static const unsigned int kFallbackNumberOfCpus = 1;
unsigned int GetNumberOfCpuCores();

template <class WorkerT>
class ConcurrentWorkers : public Observable<typename WorkerT::returned_data> {
 public:
  typedef typename WorkerT::expected_data  expected_data_t;
  typedef typename WorkerT::returned_data  returned_data_t;
  typedef typename WorkerT::worker_context worker_context_t;

 protected:
  typedef std::vector<pthread_t>           WorkerThreads;

  struct Job {
    explicit Job(const expected_data_t &data) :
      data(data),
      is_death_sentence(false) {}
    Job() :
      data(),
      is_death_sentence(true) {}

    const expected_data_t data;
    const bool is_death_sentence;
  };
  typedef std::queue<Job> JobQueue;

  struct RunBinding {
    RunBinding(ConcurrentWorkers<WorkerT> *delegate,
               const worker_context_t     *worker_context) :
      delegate(delegate),
      worker_context(worker_context) {}

    ConcurrentWorkers<WorkerT> *delegate;
    const worker_context_t     *worker_context;
  };

 public:
  ConcurrentWorkers(const size_t      number_of_workers,
                    const size_t      maximal_queue_length,
                    worker_context_t *worker_context = NULL);
  virtual ~ConcurrentWorkers();

  bool Initialize();

  inline void Schedule(const expected_data_t &data) { Schedule(Job(data)); }
  void Terminate();

  void WaitForEmptyQueue() const;
  void WaitForTermination();

  inline unsigned int GetNumberOfWorkers() const { return number_of_workers_; }

  // these methods are called by the worker objects and NOBODY else!
  inline void JobSuccessful(const returned_data_t& data) { JobDone(data, true); }
  inline void JobFailed(const returned_data_t& data) { JobDone(data, false); }

 protected:
  bool SpawnWorkers();
  static void* RunWorker(void *run_binding);

  void Schedule(Job job);
  void ScheduleDeathSentences();

  Job Acquire();
  void JobDone(const returned_data_t& data, const bool success = true);

  inline void StartRunning()    { MutexLockGuard guard(running_mutex_); running_ = true; }
  inline void StopRunning()     { MutexLockGuard guard(running_mutex_); running_ = false; }
  inline bool IsRunning() const { MutexLockGuard guard(running_mutex_); return running_; }

 private:
  ConcurrentWorkers(const ConcurrentWorkers& other) { assert(false); } // do not copy!

 private:
  // general configuration
  const size_t                 number_of_workers_;
  const size_t                 maximal_queue_length_;
  const size_t                 desired_free_slots_;
  const worker_context_t      *worker_context_;
  const RunBinding             thread_context_;

  // status information
  bool                         initialized_;
  bool                         running_;
  mutable pthread_mutex_t      running_mutex_;

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
  mutable atomic_int64         jobs_processed_;
};


template <class DerivedWorkerT>
class ConcurrentWorker {
 protected:
  ConcurrentWorker() : master_(NULL) {}
  inline ConcurrentWorkers<DerivedWorkerT>* master() const { return master_; }

 private:
  ConcurrentWorker(const ConcurrentWorker<DerivedWorkerT> &other) {
    assert (false); // do not copy!
  }

  friend class ConcurrentWorkers<DerivedWorkerT>;
  void RegisterMaster(ConcurrentWorkers<DerivedWorkerT> *master) {
    master_ = master;
  }
  virtual bool Initialize() { return true; }
  virtual void TearDown() {}

 private:
  ConcurrentWorkers<DerivedWorkerT> *master_;
};

#ifdef CVMFS_NAMESPACE_GUARD
}
#endif

#include "util_concurrency_impl.h"

#endif /* CVMFS_UTIL_CONCURRENCY_H_ */
