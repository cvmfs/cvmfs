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
 * Implements a simple interface to lock objects of derived classes. Classes that
 * inherit from Lockable are also usable with the LockGuard template for scoped
 * locking semantics.
 *
 * Note: a Lockable object should not be copied!
 */
class Lockable : SingleCopy {
 public:
  inline virtual ~Lockable() {        pthread_mutex_destroy(&mutex_); }

  void Lock()    const       {        pthread_mutex_lock(&mutex_);    }
  int  TryLock() const       { return pthread_mutex_trylock(&mutex_); }
  void Unlock()  const       {        pthread_mutex_unlock(&mutex_);  }

 protected:
  Lockable() {
    const int retval = pthread_mutex_init(&mutex_, NULL);
    assert (retval == 0);
  };

 private:
  mutable pthread_mutex_t mutex_;
};


//
// -----------------------------------------------------------------------------
//

/**
 * Used to allow for static polymorphism in the RAII template to statically
 * decide which 'lock' functions to use, if we have more than one possiblity.
 * (I.e. Read/Write locks)
 * Note: Static Polymorphism - Strategy Pattern
 *
 * TODO: eventually replace this by C++11 typed enum
 */
struct _RAII_Polymorphism {
  enum T {
    None,
    ReadLock,
    WriteLock
  };
};


/**
 * Basic template wrapper class for any kind of RAII-like behavior.
 * The user is supposed to provide a template specialization of Enter() and
 * Leave(). On creation of the RAII object it will call Enter() respectively
 * Leave() on destruction. The gold standard example is a LockGard (see below).
 *
 * Note: Resource Acquisition Is Initialization (Bjarne Stroustrup)
 */
template <typename T, _RAII_Polymorphism::T P = _RAII_Polymorphism::None>
class RAII : SingleCopy {
 public:
  inline RAII(T &object) : ref_(object)  { Enter(); }
  inline RAII(T *object) : ref_(*object) { Enter(); }
  inline ~RAII()                         { Leave(); }

 protected:
  inline void Enter() { ref_.Lock();   }
  inline void Leave() { ref_.Unlock(); }

 private:
  T &ref_;
};


/**
 * This is a simple scoped lock implementation. Every object that provides the
 * methods Lock() and Unlock() should work with it. Classes that will be used
 * with this template should therefore simply inherit from Lockable.
 *
 * Creating a LockGuard object on the stack will lock the provided object. When
 * the LockGuard runs out of scope it will automatically release the lock. This
 * ensures a clean unlock in a lot of situations!
 *
 * TODO: C++11 replace this by a type alias to RAII
 */
template <typename LockableT>
class LockGuard : public RAII<LockableT> {
 public:
  inline LockGuard(LockableT &object) : RAII<LockableT>(object) {}
  inline LockGuard(LockableT *object) : RAII<LockableT>(object) {}
};


template <>
inline void RAII<pthread_mutex_t>::Enter() { pthread_mutex_lock(&ref_);   }
template <>
inline void RAII<pthread_mutex_t>::Leave() { pthread_mutex_unlock(&ref_); }
typedef RAII<pthread_mutex_t> MutexLockGuard;


template <>
inline void RAII<pthread_rwlock_t,
                 _RAII_Polymorphism::ReadLock>::Enter() {
  pthread_rwlock_rdlock(&ref_);
}
template <>
inline void RAII<pthread_rwlock_t,
                 _RAII_Polymorphism::ReadLock>::Leave() {
  pthread_rwlock_unlock(&ref_);
}
template <>
inline void RAII<pthread_rwlock_t,
                 _RAII_Polymorphism::WriteLock>::Enter() {
  pthread_rwlock_wrlock(&ref_);
}
template <>
inline void RAII<pthread_rwlock_t,
                 _RAII_Polymorphism::WriteLock>::Leave() {
  pthread_rwlock_unlock(&ref_);
}
typedef RAII<pthread_rwlock_t, _RAII_Polymorphism::ReadLock>  ReadLockGuard;
typedef RAII<pthread_rwlock_t, _RAII_Polymorphism::WriteLock> WriteLockGuard;


//
// -----------------------------------------------------------------------------
//


/**
 * This is a simple implementation of a Future wrapper template.
 * It is used as a proxy for results that are computed asynchronously and might
 * not be available on the first access.
 * Since this is a very simple implementation one needs to use the Future's
 * Get() and Set() methods to obtain the containing data resp. to write it.
 * Note: More than a single call to Set() is prohibited!
 *       If Get() is called before Set() the calling thread will block until
 *       the value has been set by a different thread.
 *
 * @param T  the value type wrapped by this Future template
 */
template <typename T>
class Future : SingleCopy {
 public:
  Future();
  virtual ~Future();

  /**
   * Save an asynchronously computed value into the Future. This potentially
   * unblocks threads that already wait for the value.
   * @param object  the value object to be set
   */
  void     Set(const T &object);

  /**
   * Retrieves the wrapped value object. If the value is not yet available it
   * will automatically block until a different thread calls Set().
   * @return  the containing value object
   */
  T&       Get();
  const T& Get() const;

 protected:
  void Wait() const;

 private:
  T                       object_;
  mutable pthread_mutex_t mutex_;
  mutable pthread_cond_t  object_set_;
  bool                    object_was_set_;
};


//
// -----------------------------------------------------------------------------
//


/**
 * This counter can be counted up and down using the usual increment/decrement
 * operators. It allows threads to wait for it to become zero as well as to
 * block when a specified maximal value would be exceeded by an increment.
 *
 * Note: If a maximal value is specified on creation, the SynchronizingCounter
 *       is assumed to never leave the interval [0, maximal_value]! Otherwise
 *       the numerical limits of the specified template parameter define this
 *       interval and an increment _never_ blocks.
 *
 * Caveat: This implementation uses a simple mutex mechanism and therefore might
 *         become a scalability bottle neck!
 */
template <typename T>
class SynchronizingCounter : SingleCopy {
 public:
  SynchronizingCounter() :
    value_(T(0)), maximal_value_(T(0)) { Initialize(); }

  SynchronizingCounter(const T maximal_value) :
    value_(T(0)), maximal_value_(maximal_value)
  {
    assert (maximal_value > T(0));
    Initialize();
  }

  ~SynchronizingCounter() { Destroy(); }

  T Increment() {
    MutexLockGuard l(mutex_);
    WaitForFreeSlotUnprotected();
    SetValueUnprotected(value_ + T(1));
    return value_;
  }

  T Decrement() {
    MutexLockGuard l(mutex_);
    SetValueUnprotected(value_ - T(1));
    return value_;
  }

  void WaitForZero() const {
    MutexLockGuard l(mutex_);
    while (value_ != T(0)) {
      pthread_cond_wait(&became_zero_, &mutex_);
    }
    assert (value_ == T(0));
  }

  bool HasMaximalValue() const { return maximal_value_ != T(0); }
  T      maximal_value() const { return maximal_value_;         }

  T operator++()    { return Increment();        }
  T operator++(int) { return Increment() - T(1); }
  T operator--()    { return Decrement();        }
  T operator--(int) { return Decrement() + T(1); }

  operator T() const {
    MutexLockGuard l(mutex_);
    return value_;
  }

  SynchronizingCounter<T>& operator=(const T &other) {
    MutexLockGuard l(mutex_);
    SetValueUnprotected(other);
    return *this;
  }

 protected:
  void SetValueUnprotected(const T new_value);
  void WaitForFreeSlotUnprotected();

 private:
  void Initialize();
  void Destroy();

 private:
        T                 value_;
  const T                 maximal_value_;

  mutable pthread_mutex_t mutex_;
  mutable pthread_cond_t  became_zero_;
          pthread_cond_t  free_slot_;
};


//
// -----------------------------------------------------------------------------
//


template <typename ParamT>
class Observable;


/**
 * This is a base class for classes that need to expose a callback interface for
 * asynchronous callback methods. One can register an arbitrary number of
 * observers on an Observable that get notified when the method NotifyListeners()
 * is invoked.
 *
 * Note: the registration and invocation of callbacks in Observable is thread-
 *       safe, but be aware that the callbacks of observing classes might run in
 *       arbitrary threads. When using these classes, you should take extra care
 *       for thread-safety.
 *
 * Note: The RegisterListener() methods return a pointer to a CallbackBase.
 *       You MUST NOT free these objects, they are managed by the Observable
 *       class. Use them only as handles to unregister specific callbacks.
 *
 * @param ParamT   the type of the parameter that is passed to every callback
 *                 invocation.
 */
template <typename ParamT>
class Observable : public Callbackable<ParamT>,
                   SingleCopy {
 protected:
  typedef typename Callbackable<ParamT>::callback_t*  callback_ptr;
  typedef std::set<callback_ptr>                      Callbacks;

 public:
  virtual ~Observable();

  /**
   * Registers a method of a specific object as a listener to the Observable
   * object. The method is invoked on the given delegate when the callback is
   * fired by the observed object using NotifyListeners(). Since this is meant
   * to be a closure, it also passes the third argument to the method being in-
   * voked by the Observable object.
   *
   * @param DelegateT  the type of the delegate object
   * @param method     a pointer to the method to be invoked by the callback
   * @param delegate   a pointer to the object to invoke the callback on
   * @param closure    something to be passed to `method`
   * @return  a handle to the registered callback
   */
  template <class DelegateT, class ClosureDataT>
  callback_ptr RegisterListener(
          typename BoundClosure<ParamT,
                                DelegateT,
                                ClosureDataT>::CallbackMethod   method,
          DelegateT                                            *delegate,
          ClosureDataT                                          data);

  /**
   * Registers a method of a specific object as a listener to the Observable
   * object. The method is invoked on the given delegate when the callback is
   * fired by the observed object using NotifyListeners().
   *
   * @param DelegateT  the type of the delegate object
   * @param method     a pointer to the method to be invoked by the callback
   * @param delegate   a pointer to the object to invoke the callback on
   * @return  a handle to the registered callback
   */
  template <class DelegateT>
  callback_ptr RegisterListener(
          typename BoundCallback<ParamT, DelegateT>::CallbackMethod method,
          DelegateT *delegate);

  /**
   * Registers a static class member or a C-like function as a callback to the
   * Observable object. The function is invoked when the callback is fired by
   * the observed object using NotifyListeners().
   *
   * @param fn  a pointer to the function to be called by the callback
   * @return  a handle to the registered callback
   */
  callback_ptr RegisterListener(typename Callback<ParamT>::CallbackFunction fn);

  /**
   * Removes the given callback from the listeners group of this Observable.
   *
   * @param callback_object  a callback handle that was returned by
   *                         RegisterListener() before.
   */
  void UnregisterListener(callback_ptr callback_object);

  /**
   * Removes all listeners from the Observable
   */
  void UnregisterListeners();

 protected:
  Observable(); // don't instantiate this as a stand alone object

  void RegisterListener(callback_ptr callback_object);

  /**
   * Notifies all registered listeners and passes them the provided argument
   * This method should be called by a derived class to send out asynchronous
   * messages to registered observers.
   *
   * @param parameter  the data to be passed to the observers
   */
  void NotifyListeners(const ParamT &parameter);

 private:
  Callbacks                    listeners_;         //!< the set of registered
                                                   //!< callback objects
  mutable pthread_rwlock_t     listeners_rw_lock_;
};


//
// -----------------------------------------------------------------------------
//


/**
 * Returns the number of CPU cores present in the system or a fallback number
 * if it failed to determine the number of CPU cores.
 *
 * @return  the number of active CPU cores in the system
 */
unsigned int GetNumberOfCpuCores();
static const unsigned int kFallbackNumberOfCpus = 1;


//
// -----------------------------------------------------------------------------
//


/**
 * Asynchronous FIFO channel template
 * Implements a thread safe FIFO queue that handles thread blocking if the queue
 * is full or empty.
 *
 * @param T   the data type to be enqueued in the queue
 */
template <class T>
class FifoChannel : protected std::queue<T> {
 public:
  /**
   * Creates a new FIFO channel.
   *
   * @param maximal_length      the maximal number of items that can be enqueued
   * @param drainout_threshold  if less than xx elements are in the queue it is
   *                            considered to be "not full"
   */
  FifoChannel(const size_t maximal_length,
              const size_t drainout_threshold);
  virtual ~FifoChannel();

  /**
   * Adds a new item to the end of the FIFO channel. If the queue is full, this
   * call will block until items were dequeued by another thread allowing the
   * desired insertion.
   *
   * @param data  the data to be enqueued into the FIFO channel
   */
  void Enqueue(const T &data);

  /**
   * Removes the next element from the channel. If the queue is empty, this will
   * block until another thread enqueues an item into the channel.
   *
   * @return  the first item in the channel queue
   */
  const T Dequeue();

  /**
   * Clears all items in the FIFO channel. The cleared items will be lost.
   *
   * @return  the number of dropped items
   */
  unsigned int Drop();

  inline size_t GetItemCount() const;
  inline bool   IsEmpty() const;
  inline size_t GetMaximalItemCount() const;

 private:
  // general configuration
  const   size_t             maximal_queue_length_;
  const   size_t             queue_drainout_threshold_;

  // thread synchronisation structures
  mutable pthread_mutex_t    mutex_;
  mutable pthread_cond_t     queue_is_not_empty_;
  mutable pthread_cond_t     queue_is_not_full_;
};


/**
 * This template implements a generic producer/consumer approach to concurrent
 * worker tasks. It spawns a given number of Workers derived from the base class
 * ConcurrentWorker and uses them to distribute the work load onto concurrent
 * threads.
 * One can have multiple producers, that use Schedule() to post new work into
 * a central job queue, which in turn is processed concurrently by the Worker
 * objects in multiple threads. Furthermore the template provides an interface
 * to control the worker swarm, i.e. to wait for their completion or cancel them
 * before all jobs are processed.
 *
 * Note: A worker is a class inheriting from ConcurrentWorker that needs to meet
 *       a couple of requirements. See the documentation of ConcurrentWorker for
 *       additional details.
 *
 * @param WorkerT   the class to be used as a worker for a concurrent worker
 *                  swarm
 */
template <class WorkerT>
class ConcurrentWorkers : public Observable<typename WorkerT::returned_data> {
 public:
  // these data types must be defined by the worker class
  typedef typename WorkerT::expected_data  expected_data_t;  //!< input data type
  typedef typename WorkerT::returned_data  returned_data_t;  //!< output data type
  typedef typename WorkerT::worker_context worker_context_t; //!< common context type

 protected:
  typedef std::vector<pthread_t>           WorkerThreads;

  /**
   * This is a simple wrapper structure to piggy-back control information on
   * scheduled jobs. Job structures are scheduled into a central FIFO queue and
   * are then processed concurrently by the workers.
   */
  template <class DataT>
  struct Job {
    explicit Job(const DataT &data) :
      data(data),
      is_death_sentence(false) {}
    Job() :
      data(),
      is_death_sentence(true) {}
    const DataT  data;              //!< job payload
    const bool   is_death_sentence; //!< death sentence flag
  };
  typedef Job<expected_data_t> WorkerJob;
  typedef Job<returned_data_t> CallbackJob;

  /**
   * Provides a wrapper for initialization data passed to newly spawned worker
   * threads for initialization.
   * It contains a pointer to the spawning ConcurrentWorkers master object as
   * well as a pointer to a context object defined by the concrete worker to be
   * spawned.
   */
  struct RunBinding {
    RunBinding(ConcurrentWorkers<WorkerT> *delegate) :
      delegate(delegate) {}
    ConcurrentWorkers<WorkerT> *delegate;       //!< delegate to the Concurrent-
                                                //!<  Workers master
  };

  struct WorkerRunBinding : RunBinding {
    WorkerRunBinding(ConcurrentWorkers<WorkerT> *delegate,
                     const worker_context_t     *worker_context) :
      RunBinding(delegate),
      worker_context(worker_context) {}
    const worker_context_t     *worker_context; //!< WorkerT defined context ob-
                                                //!<  jects for worker init.
  };

 public:
  /**
   * Creates a ConcurrentWorkers master object that encapsulates the actual
   * workers.
   *
   * @param number_of_workers     the number of concurrent workers to be spawned
   * @param maximal_queue_length  the maximal length of the job queue
   *                              (>= number_of_workers)
   * @param worker_context        a pointer to the WorkerT defined context object
   */
  ConcurrentWorkers(const size_t      number_of_workers,
                    const size_t      maximal_queue_length,
                    worker_context_t *worker_context = NULL);
  virtual ~ConcurrentWorkers();

  /**
   * Initializes the ConcurrentWorkers swarm, spawnes a thread for each new
   * worker object and puts everything into working state.
   *
   * @return  true if all went fine
   */
  bool Initialize();

  /**
   * Schedules a new job for processing into the internal job queue. This method
   * will block in case the job queue is already full and wait for an empty slot.
   *
   * @param data  the data to be processed
   */
  inline void Schedule(const expected_data_t &data) {
    Schedule(WorkerJob(data));
  }

  /**
   * Shuts down the ConcurrentWorkers object as well as the encapsulated workers
   * as soon as possible. Workers will finish their current job and will termi-
   * nate afterwards. If no jobs are scheduled they will simply stop waiting for
   * new ones and terminate afterwards.
   * This method MUST not be called more than once per ConcurrentWorkers.
   */
  void Terminate();

  /**
   * Waits until the job queue is fully processed
   *
   * Note: this might lead to undefined behaviour or infinite waiting if other
   *       producers still schedule jobs into the job queue.
   */
  void WaitForEmptyQueue() const;

  /**
   * Waits until the ConcurrentWorkers swarm fully processed the current job
   * queue and shuts down afterwards.
   *
   * Note: just as for WaitForEmptyQueue() this assumes that no other producers
   *       schedule jobs in the mean time.
   */
  void WaitForTermination();

  inline unsigned int GetNumberOfWorkers() const { return number_of_workers_; }
  inline unsigned int GetNumberOfFailedJobs() const {
    return atomic_read32(&jobs_failed_);
  }

  /**
   * Defines a job as successfully finished.
   * DO NOT CALL THIS OUTSIDE OF A WORKER OBJECT!
   *
   * @param data  the data to be returned back to the user
   */
  inline void JobSuccessful(const returned_data_t& data) { JobDone(data, true); }

  /**
   * Defines a job as failed.
   * DO NOT CALL THIS OUTSIDE OF A WORKER OBJECT!
   *
   * Note: Even for failed jobs the user will get a callback with a data object.
   *       You might want to make sure, that this data contains a status flag as
   *       well, telling the user what went wrong.
   *
   * @param data  the data to be returned back to the user
   */
  inline void JobFailed(const returned_data_t& data) { JobDone(data, false); }

  void RunCallbackThread();

 protected:
  bool SpawnWorkers();

  /**
   * POSIX conform function for thread entry point. Is invoked for every new
   * worker thread and contains the initialization, processing loop and tear
   * down of the unique worker objects
   *
   * @param run_binding  void pointer to a RunBinding structure (C interface)
   * @return  NULL in any case
   */
  static void* RunWorker(void *run_binding);

  static void* RunCallbackThreadWrapper(void *run_binding);

  /**
   * Tells the master that a worker thread did start. This does not mean, that
   * it was initialized successfully.
   */
  void ReportStartedWorker() const;

  void Schedule(WorkerJob job);
  void ScheduleDeathSentences();

  /**
   * Empties the job queue
   *
   * @param forget_pending  controls if cancelled jobs should be seen as finished
   */
  void TruncateJobQueue(const bool forget_pending = false);

  /**
   * Retrieves a job from the job queue. If the job queue is empty it will block
   * until there is a new job available for processing.
   * THIS METHOD MUST ONLY BE CALLED INSIDE THE WORKER OBJECTS
   *
   * @return  a job to be processed by a worker
   */
  inline WorkerJob Acquire();

  /**
   * Controls the asynchronous finishing of a job.
   * DO NOT CALL THIS, use JobSuccessful() or JobFailed() wrappers instead.
   *
   * @param data     the data to be returned to the user
   * @param success  flag if job was successful
   */
  void JobDone(const returned_data_t& data, const bool success = true);

  inline void StartRunning()    {
    MutexLockGuard guard(status_mutex_);
    running_ = true;
  }
  inline void StopRunning()     {
    MutexLockGuard guard(status_mutex_);
    running_ = false;
  }
  inline bool IsRunning() const {
    MutexLockGuard guard(status_mutex_);
    return running_;
  }

 private:
  // general configuration
  const size_t             number_of_workers_;    //!< number of concurrent
                                                  //!<  worker threads
  const worker_context_t  *worker_context_;       //!< the WorkerT defined context
  const WorkerRunBinding   thread_context_;       //!< the thread context passed
                                                  //!<  to newly spawned threads

  // status information
  bool                     initialized_;
  bool                     running_;
  mutable unsigned int     workers_started_;
  mutable pthread_mutex_t  status_mutex_;
  mutable pthread_cond_t   worker_started_;
  mutable pthread_mutex_t  jobs_all_done_mutex_;
  mutable pthread_cond_t   jobs_all_done_;

  // worker threads
  WorkerThreads            worker_threads_;       //!< list of worker threads
  pthread_t                callback_thread_;      //!< handles callback invokes

  // job queue
  typedef FifoChannel<WorkerJob > JobQueue;
  JobQueue                 jobs_queue_;
  mutable atomic_int32     jobs_pending_;
  mutable atomic_int32     jobs_failed_;
  mutable atomic_int64     jobs_processed_;

  // callback channel
  typedef FifoChannel<CallbackJob > CallbackQueue;
  CallbackQueue results_queue_;
};


/**
 * Base class for worker classes that should be used in a ConcurrentWorkers
 * swarm. These classes need to fulfill a number of requirements in order to
 * satisfy the needs of the ConcurrentWorkers template.
 *
 * Requirements:
 *  -> needs to define the following types:
 *       - expected_data   - input data structure of the worker
 *       - returned_data   - output data structure of the worker
 *       - worker_context  - context structure for initialization information
 *
 *  -> implement a constructor that takes a pointer to its worker_context
 *     as its only parameter:
 *        AwesomeWorker(const AwesomeWorker::worker_context*)
 *     Note: do not rely on the context object to be available after the
 *           consturctor has returned!
 *
 *  -> needs to define the calling-operator expecting one parameter of type:
 *     const expected_data& and returning void
 *     This will be invoked for every new job the worker should process
 *
 *  -> inside the implementation of the described calling-operator it needs to
 *     invoke either:
 *        master()->JobSuccessful(const returned_data&)
 *     or:
 *        master()->JobFailed(const returned_data&)
 *     as its LAST operation before returning.
 *     This will keep track of finished jobs and inform the user of Concurrent-
 *     Workers about finished jobs.
 *
 *  -> [optional] overwrite Initialize() and/or TearDown() to do environmental
 *     setup work, before or respectively after jobs will be processed
 *
 * General Reminder:
 *   You will be running in a multi-threaded environment here! Buckle up and
 *   make suitable preparations to shield yourself from serious head-ache.
 *
 * Note: This implements a Curiously Recurring Template Pattern
 *       (http://en.wikipedia.org/wiki/Curiously_recurring_template_pattern)
 *
 * @param DerivedWorkerT  the class name of the inheriting class
 *        (f.e.   class AwesomeWorker : public ConcurrentWorker<AwesomeWorker>)
 */
template <class DerivedWorkerT>
class ConcurrentWorker : SingleCopy {
 public:
  virtual ~ConcurrentWorker() {}

  /**
   * Does general initialization before any jobs will get scheduled. You do not
   * need to up-call this initialize method, since it is seen as a dummy here.
   *
   * @returns  true one successful initialization
   */
  virtual bool Initialize() { return true; }

  /**
   * Does general clean-up after the last job was processed in the worker object
   * and it is about to vanish. You do not need to up-call this method.
   */
  virtual void TearDown() {}

  /**
   * The actual job-processing entry point. See the description of the inheriting
   * class requirements to learn about the semantics of this methods.
   * DO NOT FORGET TO CALL master()->JobSuccessful() OR master()->JobFinished()
   * at the end of thismethod!!
   *
   * Note: There is no way to generally define this operator, it is therefore
   *       commented out and placed here just as a reminder.
   *
   * @param data  the data to be processed.
   */
  //void operator()(const expected_data &data); // do the actual job of the
                                                // worker

 protected:
  ConcurrentWorker() : master_(NULL) {}

  /**
   * Gets a pointer to the ConcurrentWorkers object that this worker resides in
   *
   * @returns  a pointer to the ConcurrentWorkers object
   */
  inline ConcurrentWorkers<DerivedWorkerT>* master() const { return master_; }

 private:
  friend class ConcurrentWorkers<DerivedWorkerT>;
  void RegisterMaster(ConcurrentWorkers<DerivedWorkerT> *master) {
    master_ = master;
  }

 private:
  ConcurrentWorkers<DerivedWorkerT> *master_;
};

#ifdef CVMFS_NAMESPACE_GUARD
}
#endif

#include "util_concurrency_impl.h"

#endif /* CVMFS_UTIL_CONCURRENCY_H_ */
