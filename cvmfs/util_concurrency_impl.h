/**
 * This file is part of the CernVM File System.
 */

#include "util.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

template <class WorkerT>
ConcurrentWorkers<WorkerT>::ConcurrentWorkers(const size_t      number_of_workers,
                                              const size_t      maximal_queue_length,
                                              worker_context_t *worker_context) :
  number_of_workers_(number_of_workers),
  maximal_queue_length_(maximal_queue_length),
  desired_free_slots_(maximal_queue_length / 2 + 1),
  worker_context_(worker_context),
  initialized_(false),
  running_(false)
{
  assert (maximal_queue_length_ > 0);
  assert (desired_free_slots_   > 0);
  assert (number_of_workers     > 0);
}


template <class WorkerT>
ConcurrentWorkers<WorkerT>::~ConcurrentWorkers() {
  if (running_) {
    Terminate();
  }

  // remove listener callbacks
  UnregisterListeners();

  // destroy some synchronisation data structures
  pthread_cond_destroy(&job_queue_cond_not_full_);
  pthread_cond_destroy(&job_queue_cond_not_empty_);
  pthread_cond_destroy(&jobs_all_done_);
  pthread_mutex_destroy(&job_queue_mutex_);
  pthread_mutex_destroy(&listeners_mutex_);
}


template <class WorkerT>
bool ConcurrentWorkers<WorkerT>::Initialize() {
  LogCvmfs(kLogConcurrency, kLogVerboseMsg, "Initializing ConcurrentWorker "
                                            "object with %d worker threads "
                                            "and a queue length of %d",
           number_of_workers_, maximal_queue_length_);

  // initialize synchronisation for job queue (Workers)
  if (pthread_mutex_init(&listeners_mutex_, NULL)         != 0) return false;
  if (pthread_mutex_init(&job_queue_mutex_, NULL)         != 0) return false;
  if (pthread_cond_init(&job_queue_cond_not_full_, NULL)  != 0) return false;
  if (pthread_cond_init(&job_queue_cond_not_empty_, NULL) != 0) return false;
  if (pthread_cond_init(&jobs_all_done_, NULL)            != 0) return false;

  // spawn the Worker objects in their own threads
  running_ = true;
  if (!SpawnWorkers()) {
    LogCvmfs(kLogConcurrency, kLogWarning, "Failed to spawn workers");
    return false;
  }

  initialized_ = true;
  return true;
}


template <class WorkerT>
bool ConcurrentWorkers<WorkerT>::SpawnWorkers() {
  assert (worker_threads_.size() == 0);
  worker_threads_.resize(number_of_workers_);

  // spawn the swarm and make them work
  bool success = true;
  WorkerThreads::iterator i          = worker_threads_.begin();
  WorkerThreads::const_iterator iend = worker_threads_.end();
  for (; i != iend; ++i) {
    pthread_t* thread = &(*i);
    const int retval = pthread_create(thread,
                                      NULL,
                                      &ConcurrentWorkers<WorkerT>::RunWorker,
                                      pushworker_context_);
    if (retval != 0) {
      LogCvmfs(kLogConcurrency, kLogWarning, "Failed to spawn a Worker");
      success = false;
    }
  }

  // all done...
  return success;
}


template <class WorkerT>
void* ConcurrentWorkers<WorkerT>::RunWorker(void *run_binding) {
  //
  // INITIALIZATION
  /////////////////

  RunBinding *binding = static_cast<RunBinding*>(run_binding);
  ConcurrentWorkers<WorkerT> *delegate       = binding->delegate;
  worker_context_t           *worker_context = binding->worker_context;
  delete binding;

  // boot up the push worker object and make sure it works
  WorkerT worker(worker_context, delegate);
  if (! worker.Initialize()) {
    LogCvmfs(kLogConcurrency, kLogWarning, "Worker was not initialized "
                                           "properly... it will die now!");
    return NULL; 
  }

  //
  // PROCESSING LOOP
  //////////////////

  // start the processing loop
  LogCvmfs(kLogConcurrency, kLogVerboseMsg, "Starting Worker...");
  while (master->is_running()) {
    expected_data_t& job = delegate->AcquireJob();
    worker(job);
  }

  //
  // TEAR DOWN
  ////////////

  // good bye thread...
  LogCvmfs(kLogConcurrency, kLogVerboseMsg, "Terminating Worker...");
  return NULL;
}


template <class WorkerT>
void ConcurrentWorkers<WorkerT>::Schedule(const expected_data_t &data) {
  // Note: This method can be called from arbitrary threads. Thus we do not
  //       necessarily have just one producer in the system.

  LogCvmfs(kLogConcurrency, kLogVerboseMsg, "scheduling new job");

  // lock the job queue
  PosixLockGuard guard(job_queue_mutex_);

  // wait until there is space in the job queue
  while (job_queue_.size() >= maximal_queue_length_) {
    pthread_cond_wait(&job_queue_cond_not_full_, &job_queue_mutex_);
  }

  // put something into the job queue
  job_queue_.push(data);
  atomic_inc32(&jobs_pending_);

  // wake all waiting threads
  pthread_cond_broadcast(&job_queue_cond_not_empty_);
}


template <class WorkerT>
expected_data_t ConcurrentWorkers<WorkerT>::Acquire() {
  // Note: This method is exclusively called inside the worker threads!
  //       Any other usage might produce undefined behavior.

  // lock the job queue
  PosixLockGuard guard(job_queue_mutex_);

  // wait until there is something to do
  while (job_queue_.empty()) {
    pthread_cond_wait(&job_queue_cond_not_empty_, &job_queue_mutex_);
    if (!running_) {
      // we were asked to commit suicide...
      pthread_exit(NULL);
    }
  }

  // get the job and remove it from the queue
  const expected_data_t& job = job_queue_.front(); // TODO: need to copy here
                               job_queue_.pop();   //       C++11 move semantics? don't really know...

  // signal the Scheduler that there is a fair amount of free space now
  if (job_queue_.size() < desired_free_slots_) {
    pthread_cond_signal(&job_queue_cond_not_full_);
  }

  // return the acquired job
  LogCvmfs(kLogConcurrency, kLogVerboseMsg, "acquired a job");
  return job;
}


template <class WorkerT>
void ConcurrentWorkers<WorkerT>::Terminate() {
  // Note: this method causes threads to die immediately after they finished
  //       their last acquired job or when they are in idle state.

  // lock the job queue
  PosixLockGuard guard(job_queue_mutex_);

  // unset the running flag (causing threads to die on the next check point)
  running_ = false;

  // send broadcast to wake up possible idle threads
  // they will first check the running_ flag and die immediately
  pthread_cond_broadcast(&job_queue_cond_not_empty_);

  // wait for the worker threads to return
  WorkerThreads::const_iterator i    = worker_threads_.begin();
  WorkerThreads::const_iterator iend = worker_threads_.end();
  for (; i != iend; ++i) {
    pthread_join(*i, NULL);
  }

  // check if we finished all pending jobs
  const int pending = atomic_read32(&jobs_pending_);
  if (pending > 0) {
    LogCvmfs(kLogConcurrency, kLogWarning, "Job queue was not fully processed. "
                                           "Still %d jobs were pending and "
                                           "will not be executed anymore.",
             pending);
  }

  // check if we had failed jobs
  const int failed = atomic_read32(&jobs_failed_);
  if (failed > 0) {
    LogCvmfs(kLogConcurrency, kLogWarning, "We've had %d failed jobs.",
             failed);
  }

  // thanks, and good bye...
  LogCvmfs(kLogConcurrency, kLogVerboseMsg, "All workers stopped. Terminating...");
}


template <class WorkerT>
void ConcurrentWorkers<WorkerT>::WaitForEmptyQueue() const {
  // lock the job queue
  PosixLockGuard guard(job_queue_mutex_);

  LogCvmfs(kLogConcurrency, kLogVerboseMsg, "Waiting for all jobs to be finished...");

  // wait until all pending jobs are processed
  while (atomic_read32(&jobs_pending_) > 0) {
    pthread_cond_wait(&jobs_all_done_, &job_queue_mutex_);
  }

  LogCvmfs(kLogConcurrency, kLogVerboseMsg, "Jobs are done... go on");
}


template <class WorkerT>
void ConcurrentWorkers<WorkerT>::WaitForTermination() const {
  WaitForEmptyQueue();
  Terminate();
}


template <class WorkerT>
void ConcurrentWorkers<WorkerT>::RegisterListener(callback_t *callback_object) {
  PosixLockGuard guard(listeners_mutex_);
  listeners_.insert(callback_object);
}


template <class WorkerT>
void ConcurrentWorkers<WorkerT>::UnregisterListener(callback_t *callback_object) {
  PosixLockGuard guard(listeners_mutex_);
  listeners_.erase(callback_object);
}


template <class WorkerT>
void ConcurrentWorkers<WorkerT>::UnregisterListeners() {
  PosixLockGuard guard(listeners_mutex_);
  Callbacks::const_iterator i    = listeners_.begin();
  Callbacks::const_iterator iend = listeners_.end();
  for (; i != iend; ++i) {
    delete *i;
  }
  listeners_.clear();
}


template <class WorkerT>
void ConcurrentWorkers<WorkerT>::JobDone(const returned_data_t& data,
                                         const bool             success) {
  // BEWARE!
  // This is a callback method that might be called from a different thread!

  // check if the finished job was successful
  if (success) {
    LogCvmfs(kLogConcurrency, kLogVerboseMsg, "Job succeeded";
  } else {
    atomic_inc32(&jobs_failed_);
    LogCvmfs(kLogConcurrency, kLogWarning, "Job failed");
  }

  // call all callback objects and inform our groupies...
  {
    PosixLockGuard guard(listeners_mutex_);
    Callbacks::const_iterator i    = listeners_.begin();
    Callbacks::const_iterator iend = listeners_.end();
    for (; i != iend; ++i) {
      (**i)(data);
    }
  }

  // remove the finished job from the pending 'list'
  atomic_dec32(&jobs_pending_);

  // Signal the Spooler that all jobs are done...
  if (atomic_read32(&jobs_pending_) == 0) {
    pthread_cond_signal(&jobs_all_done_);
  }
}


#ifdef CVMFS_NAMESPACE_GUARD
}
#endif
