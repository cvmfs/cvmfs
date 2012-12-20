/**
 * This file is part of the CernVM File System.
 */

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif


//
// +----------------------------------------------------------------------------
// |  Observable
//


template <typename ParamT>
Observable<ParamT>::Observable() {
  const int ret = pthread_rwlock_init(&listeners_rw_lock_, NULL);
  assert (ret == 0);
}


template <typename ParamT>
Observable<ParamT>::~Observable() {
  UnregisterListeners();
  pthread_rwlock_destroy(&listeners_rw_lock_);
}


template <typename ParamT>
template <class DelegateT>
typename Observable<ParamT>::callback_t Observable<ParamT>::RegisterListener(
    typename BoundCallback<ParamT, DelegateT>::CallbackMethod method,
    DelegateT *delegate) {
  // create a new BoundCallback, register it and return the handle
  CallbackBase<ParamT> *callback =
    new BoundCallback<ParamT, DelegateT>(method, delegate);
  RegisterListener(callback);
  return callback;
}


template <typename ParamT>
typename Observable<ParamT>::callback_t Observable<ParamT>::RegisterListener(
    typename Callback<ParamT>::CallbackFunction fn) {
  // create a new Callback, register it and return the handle
  CallbackBase<ParamT> *callback =
    new Callback<ParamT>(fn);
  RegisterListener(callback);
  return callback;
}


template <typename ParamT>
void Observable<ParamT>::RegisterListener(
    Observable<ParamT>::callback_t callback_object) {
  // register a generic CallbackBase callback
  WriteLockGuard guard(listeners_rw_lock_);
  listeners_.insert(callback_object);
}


template <typename ParamT>
void Observable<ParamT>::UnregisterListener(
    Observable<ParamT>::callback_t callback_object) {
  // remove a callback handle from the callbacks list
  // if it is not registered --> crash
  WriteLockGuard guard(listeners_rw_lock_);
  const size_t was_removed = listeners_.erase(callback_object);
  assert (was_removed);
  delete callback_object;
}


template <typename ParamT>
void Observable<ParamT>::UnregisterListeners() {
  WriteLockGuard guard(listeners_rw_lock_);

  // remove all callbacks from the list
  typename Callbacks::const_iterator i    = listeners_.begin();
  typename Callbacks::const_iterator iend = listeners_.end();
  for (; i != iend; ++i) {
    delete *i;
  }
  listeners_.clear();
}


template <typename ParamT>
void Observable<ParamT>::NotifyListeners(const ParamT &parameter) {
  ReadLockGuard guard(listeners_rw_lock_);

  // invoke all callbacks and inform them about new data
  typename Callbacks::const_iterator i    = listeners_.begin();
  typename Callbacks::const_iterator iend = listeners_.end();
  for (; i != iend; ++i) {
    (**i)(parameter);
  }
}


//
// +----------------------------------------------------------------------------
// |  ConcurrentWorkers
//


template <class WorkerT>
ConcurrentWorkers<WorkerT>::ConcurrentWorkers(
          const size_t                                  number_of_workers,
          const size_t                                  maximal_queue_length,
          ConcurrentWorkers<WorkerT>::worker_context_t *worker_context) :
  number_of_workers_(number_of_workers),
  maximal_queue_length_(maximal_queue_length),
  desired_free_slots_(maximal_queue_length / 2 + 1), // TODO: consider to remove this magic numbers
  worker_context_(worker_context),
  thread_context_(this, worker_context_),
  initialized_(false),
  running_(false)
{
  assert (maximal_queue_length_ >= number_of_workers_);
  assert (desired_free_slots_   >  0);
  assert (number_of_workers     >  0);

  atomic_init32(&jobs_pending_);
  atomic_init32(&jobs_failed_);
  atomic_init64(&jobs_processed_);
}


template <class WorkerT>
ConcurrentWorkers<WorkerT>::~ConcurrentWorkers() {
  if (IsRunning()) {
    Terminate();
  }

  // destroy some synchronisation data structures
  pthread_cond_destroy(&job_queue_cond_not_full_);
  pthread_cond_destroy(&job_queue_cond_not_empty_);
  pthread_cond_destroy(&jobs_all_done_);
  pthread_mutex_destroy(&running_mutex_);
  pthread_mutex_destroy(&job_queue_mutex_);
}


template <class WorkerT>
bool ConcurrentWorkers<WorkerT>::Initialize() {
  LogCvmfs(kLogConcurrency, kLogVerboseMsg, "Initializing ConcurrentWorker "
                                            "object with %d worker threads "
                                            "and a queue length of %d",
           number_of_workers_, maximal_queue_length_);

  // initialize synchronisation for job queue (Workers)
  if (pthread_mutex_init(&job_queue_mutex_, NULL)         != 0 ||
      pthread_mutex_init(&running_mutex_, NULL)           != 0 ||
      pthread_cond_init(&job_queue_cond_not_full_, NULL)  != 0 ||
      pthread_cond_init(&job_queue_cond_not_empty_, NULL) != 0 ||
      pthread_cond_init(&jobs_all_done_, NULL)            != 0) return false;

  // spawn the Worker objects in their own threads
  if (!SpawnWorkers()) {
    LogCvmfs(kLogConcurrency, kLogWarning, "Failed to spawn workers");
    return false;
  }

  // all done...
  initialized_ = true;
  return true;
}


template <class WorkerT>
bool ConcurrentWorkers<WorkerT>::SpawnWorkers() {
  assert (worker_threads_.size() == 0);
  worker_threads_.resize(number_of_workers_);

  // set the running flag to trap workers in their treadmills
  StartRunning();

  // spawn the swarm and make them work
  bool success = true;
  WorkerThreads::iterator i          = worker_threads_.begin();
  WorkerThreads::const_iterator iend = worker_threads_.end();
  for (; i != iend; ++i) {
    pthread_t* thread = &(*i);
    const int retval = pthread_create(thread,
                                      NULL,
                                      &ConcurrentWorkers<WorkerT>::RunWorker,
                               (void*)&thread_context_);
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
  // NOTE: This is the actual worker thread code!

  //
  // INITIALIZATION
  /////////////////

  // get contextual information
  const RunBinding binding = *(static_cast<RunBinding*>(run_binding));
  ConcurrentWorkers<WorkerT> *master         = binding.delegate;
  const worker_context_t     *worker_context = binding.worker_context;

  // boot up the worker object and make sure it works
  WorkerT worker(worker_context);
  worker.RegisterMaster(master);
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
  while (master->IsRunning()) {
    // acquire a new job
    Job job = master->Acquire();

    // check if we need to terminate
    if (job.is_death_sentence)
      break;

    // do what you are supposed to do
    worker(job.data);
  }

  //
  // TEAR DOWN
  ////////////

  // give the worker the chance to tidy up
  worker.TearDown();

  // good bye thread...
  LogCvmfs(kLogConcurrency, kLogVerboseMsg, "Terminating Worker...");
  return NULL;
}


template <class WorkerT>
void ConcurrentWorkers<WorkerT>::Schedule(Job job) {
  // Note: This method can be called from arbitrary threads. Thus we do not
  //       necessarily have just one producer in the system.

  // check if it makes sense to schedule this job
  if (!IsRunning() && !job.is_death_sentence) {
    LogCvmfs(kLogConcurrency, kLogWarning, "Tried to schedule a job but "
                                           "concurrency was not running...");
    return;
  }

  // lock the job queue
  {
    MutexLockGuard guard(job_queue_mutex_);

    // wait until there is space in the job queue
    while (job_queue_.size() >= maximal_queue_length_) {
      pthread_cond_wait(&job_queue_cond_not_full_, &job_queue_mutex_);
    }

    // put something into the job queue
    job_queue_.push(job);
    if (!job.is_death_sentence) {
      atomic_inc32(&jobs_pending_);
    }
  }

  // wake all waiting threads
  pthread_cond_broadcast(&job_queue_cond_not_empty_);
}


template <class WorkerT>
void ConcurrentWorkers<WorkerT>::ScheduleDeathSentences() {
  assert (!IsRunning());

  // make sure that the queue is empty before we schedule a death sentence
  TruncateJobQueue();

  // schedule a death sentence for each running thread
  const unsigned int number_of_workers = GetNumberOfWorkers();
  for (unsigned int i = 0; i < number_of_workers; ++i) {
    Schedule(Job());
  }
}


template <class WorkerT>
typename ConcurrentWorkers<WorkerT>::Job ConcurrentWorkers<WorkerT>::Acquire() {
  // Note: This method is exclusively called inside the worker threads!
  //       Any other usage might produce undefined behavior.

  // lock the job queue
  MutexLockGuard guard(job_queue_mutex_);

  // wait until there is something to do
  while (job_queue_.empty()) {
    pthread_cond_wait(&job_queue_cond_not_empty_, &job_queue_mutex_);
  }

  // get the job and remove it from the queue
  Job job = job_queue_.front();
  job_queue_.pop();

  // signal the Scheduler that there is a fair amount of free space now
  if (job_queue_.size() < desired_free_slots_) {
    pthread_cond_broadcast(&job_queue_cond_not_full_);
  }

  // return the acquired job
  return job;
}


template <class WorkerT>
void ConcurrentWorkers<WorkerT>::TruncateJobQueue(const bool forget_pending) {
  // Note: This method will throw away all jobs currently waiting in the job
  //       queue. These jobs will not be processed!

  // lock the job queue
  {
    MutexLockGuard guard(job_queue_mutex_);

    // clear the job queue
    while (!job_queue_.empty()) job_queue_.pop();

    // if desired, we remove the jobs from the pending 'list'
    if (forget_pending) {
      const int pending = atomic_read32(&jobs_pending_);
      atomic_xadd32(&jobs_pending_, -pending);
    }
  }

  // signal the scheduler that the queue is now empty
  pthread_cond_broadcast(&job_queue_cond_not_full_);
}


template <class WorkerT>
void ConcurrentWorkers<WorkerT>::Terminate() {
  // Note: this method causes workers to die immediately after they finished
  //       their last acquired job. To make sure that each worker will check
  //       the running state, we schedule empty jobs or Death Sentences.

  assert (IsRunning());

  // unset the running flag (causing threads to die on the next checkpoint)
  StopRunning();

  // schedule empty jobs to make sure that each worker will actually reach the
  // next checkpoint in their processing loop and terminate as expected
  ScheduleDeathSentences();

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
  LogCvmfs(kLogConcurrency, kLogVerboseMsg, "All workers stopped. They processed "
                                            "%d jobs. Terminating...",
           atomic_read64(&jobs_processed_));
}


template <class WorkerT>
void ConcurrentWorkers<WorkerT>::WaitForEmptyQueue() const {
  LogCvmfs(kLogConcurrency, kLogVerboseMsg, "Waiting for %d jobs to be finished",
           atomic_read32(&jobs_pending_));

  // lock the job queue
  {
    MutexLockGuard guard(job_queue_mutex_);

    // wait until all pending jobs are processed
    while (atomic_read32(&jobs_pending_) > 0) {
      pthread_cond_wait(&jobs_all_done_, &job_queue_mutex_);
    }
  }

  LogCvmfs(kLogConcurrency, kLogVerboseMsg, "Jobs are done... go on");
}


template <class WorkerT>
void ConcurrentWorkers<WorkerT>::WaitForTermination() {
  if (!IsRunning())
    return;

  WaitForEmptyQueue();
  Terminate();
}


template <class WorkerT>
void ConcurrentWorkers<WorkerT>::JobDone(
              const ConcurrentWorkers<WorkerT>::returned_data_t& data,
              const bool                                         success) {
  // BEWARE!
  // This is a callback method that might be called from a different thread!

  // check if the finished job was successful
  if (!success) {
    atomic_inc32(&jobs_failed_);
    LogCvmfs(kLogConcurrency, kLogWarning, "Job failed");
  }

  // notify all observers about the finished job
  this->NotifyListeners(data);
  
  // remove the job from the pending 'list' and add it to the ready 'list'
  atomic_dec32(&jobs_pending_);
  atomic_inc64(&jobs_processed_);

  // Signal the Spooler that all jobs are done...
  if (atomic_read32(&jobs_pending_) == 0) {
    pthread_cond_broadcast(&jobs_all_done_);
  }
}


#ifdef CVMFS_NAMESPACE_GUARD
}
#endif
