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
typename Observable<ParamT>::callback_ptr Observable<ParamT>::RegisterListener(
    typename BoundCallback<ParamT, DelegateT>::CallbackMethod method,
    DelegateT *delegate) {
  // create a new BoundCallback, register it and return the handle
  CallbackBase<ParamT> *callback =
    Observable<ParamT>::MakeCallback(method, delegate);
  RegisterListener(callback);
  return callback;
}


template <typename ParamT>
typename Observable<ParamT>::callback_ptr Observable<ParamT>::RegisterListener(
    typename Callback<ParamT>::CallbackFunction fn) {
  // create a new Callback, register it and return the handle
  CallbackBase<ParamT> *callback =
    Observable<ParamT>::MakeCallback(fn);
  RegisterListener(callback);
  return callback;
}


template <typename ParamT>
void Observable<ParamT>::RegisterListener(
    Observable<ParamT>::callback_ptr callback_object) {
  // register a generic CallbackBase callback
  WriteLockGuard guard(listeners_rw_lock_);
  listeners_.insert(callback_object);
}


template <typename ParamT>
void Observable<ParamT>::UnregisterListener(
    typename Observable<ParamT>::callback_ptr callback_object) {
  // remove a callback handle from the callbacks list
  // if it is not registered --> crash
  WriteLockGuard guard(listeners_rw_lock_);
  const size_t was_removed = listeners_.erase(callback_object);
  assert (was_removed > 0);
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
// |  FifoChannel
//


template <class T>
FifoChannel<T>::FifoChannel(const size_t maximal_length,
                            const size_t drainout_threshold,
                            const size_t prefill_threshold) :
  maximal_queue_length_(maximal_length),
  queue_drainout_threshold_(drainout_threshold),
  queue_prefill_threshold_(prefill_threshold),
  drainout_mode_(false)
{
  assert (drainout_threshold <= maximal_length);
  assert (drainout_threshold >  0);
  assert (prefill_threshold  >= 0);
  assert (prefill_threshold  <  maximal_length);

  const bool successful = (
    pthread_mutex_init(&mutex_, NULL)              == 0 &&
    pthread_cond_init(&queue_is_not_empty_, NULL)  == 0 &&
    pthread_cond_init(&queue_is_not_full_, NULL)   == 0
  );

  assert (successful);
}


template <class T>
FifoChannel<T>::~FifoChannel() {
  pthread_cond_destroy(&queue_is_not_empty_);
  pthread_cond_destroy(&queue_is_not_full_);
  pthread_mutex_destroy(&mutex_);
}


template <class T>
void FifoChannel<T>::Enqueue(const T &data) {
  MutexLockGuard lock(mutex_);
  //LogCvmfs(kLogSpooler, kLogStdout, "--> ENQUEUED (%d): %d", sizeof(T), this->size());

  // wait for space in the queue
  while (this->size() >= maximal_queue_length_) {
    pthread_cond_wait(&queue_is_not_full_, &mutex_);
  }

  // put something into the queue
  this->push(data);

  // wake all waiting threads
  if (drainout_mode_ || this->size() > queue_prefill_threshold_) {
    pthread_cond_broadcast(&queue_is_not_empty_);
  }
}


template <class T>
const T FifoChannel<T>::Dequeue() {
  MutexLockGuard lock(mutex_);
  //LogCvmfs(kLogSpooler, kLogStdout, "--> DEQUEUED (%d): %d", sizeof(T), this->size());

  // wait until there is something to do
  while (this->empty()) {
    pthread_cond_wait(&queue_is_not_empty_, &mutex_);
  }

  // get the item from the queue
  T data = this->front(); this->pop();

  // signal waiting threads about the free space
  if (this->size() < queue_drainout_threshold_) {
    pthread_cond_broadcast(&queue_is_not_full_);
  }

  // return the acquired job
  return data;
}


template <class T>
unsigned int FifoChannel<T>::Drop() {
  MutexLockGuard lock(mutex_);

  unsigned int dropped_items = 0;
  while (!this->empty()) {
    this->pop();
    ++dropped_items;
  }

  pthread_cond_broadcast(&queue_is_not_full_);

  return dropped_items;
}


template <class T>
size_t FifoChannel<T>::GetItemCount() const {
  MutexLockGuard lock(mutex_);
  return this->size();
}


template <class T>
bool FifoChannel<T>::IsEmpty() const {
  MutexLockGuard lock(mutex_);
  return this->empty();
}


template <class T>
size_t FifoChannel<T>::GetMaximalItemCount() const {
  return maximal_queue_length_;
}


template <class T>
void FifoChannel<T>::EnableDrainoutMode() const {
  drainout_mode_ = true;
  pthread_cond_broadcast(&queue_is_not_empty_);
}


template <class T>
void FifoChannel<T>::DisableDrainoutMode() const {
  drainout_mode_ = false;
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
  worker_context_(worker_context),
  thread_context_(this, worker_context_),
  initialized_(false),
  running_(false),
  workers_started_(0),
  jobs_queue_(maximal_queue_length, maximal_queue_length / 4 + 1),
  results_queue_(maximal_queue_length, 1, maximal_queue_length - 1)
{
  assert (maximal_queue_length  >= number_of_workers);
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
  pthread_cond_destroy(&worker_started_);
  pthread_cond_destroy(&jobs_all_done_);
  pthread_mutex_destroy(&status_mutex_);
  pthread_mutex_destroy(&jobs_all_done_mutex_);
}


template <class WorkerT>
bool ConcurrentWorkers<WorkerT>::Initialize() {
  LogCvmfs(kLogConcurrency, kLogVerboseMsg, "Initializing ConcurrentWorker "
                                            "object with %d worker threads "
                                            "and a queue length of %d",
           number_of_workers_, jobs_queue_.GetMaximalItemCount());
  // LogCvmfs(kLogConcurrency, kLogStdout, "sizeof(expected_data_t): %d\n"
  //                                           "sizeof(returned_data_t): %d",
  //          sizeof(expected_data_t), sizeof(returned_data_t));

  // initialize synchronisation for job queue (Workers)
  if (pthread_mutex_init(&status_mutex_, NULL)          != 0 ||
      pthread_mutex_init(&jobs_all_done_mutex_, NULL)   != 0 ||
      pthread_cond_init(&worker_started_, NULL)         != 0 ||
      pthread_cond_init(&jobs_all_done_, NULL)          != 0) {
    return false;
  }

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

  // spawn the callback processing thread
  pthread_create(&callback_thread_,
                 NULL,
                 &ConcurrentWorkers<WorkerT>::RunCallbackThreadWrapper,
          (void*)&thread_context_);

  // wait for all workers to report in...
  {
    MutexLockGuard guard(status_mutex_);

    while (workers_started_ < number_of_workers_ + 1) { // +1 -> callback thread
      pthread_cond_wait(&worker_started_, &status_mutex_);
    }
  }

  // all done...
  return success;
}


template <class WorkerT>
void ConcurrentWorkers<WorkerT>::EnableDrainoutMode() const {
  jobs_queue_.EnableDrainoutMode();
  results_queue_.EnableDrainoutMode();
}


template <class WorkerT>
void ConcurrentWorkers<WorkerT>::DisableDrainoutMode() const {
  jobs_queue_.DisableDrainoutMode();
  results_queue_.DisableDrainoutMode();
}


template <class WorkerT>
void* ConcurrentWorkers<WorkerT>::RunWorker(void *run_binding) {
  // NOTE: This is the actual worker thread code!

  //
  // INITIALIZATION
  /////////////////

  // get contextual information
  const WorkerRunBinding &binding =
    *(static_cast<WorkerRunBinding*>(run_binding));
  ConcurrentWorkers<WorkerT> *master         = binding.delegate;
  const worker_context_t     *worker_context = binding.worker_context;

  // boot up the worker object and make sure it works
  WorkerT worker(worker_context);
  worker.RegisterMaster(master);
  const bool init_success = worker.Initialize();

  // tell the master that this worker was started
  master->ReportStartedWorker();

  if (!init_success) {
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
    WorkerJob job = master->Acquire();

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
void* ConcurrentWorkers<WorkerT>::RunCallbackThreadWrapper(void *run_binding) {
  const RunBinding           &binding = *(static_cast<RunBinding*>(run_binding));
  ConcurrentWorkers<WorkerT> *master  = binding.delegate;

  master->ReportStartedWorker();

  LogCvmfs(kLogConcurrency, kLogVerboseMsg, "Started dedicated callback worker");
  master->RunCallbackThread();
  LogCvmfs(kLogConcurrency, kLogVerboseMsg, "Terminating Callback Worker...");

  return NULL;
}


template <class WorkerT>
void ConcurrentWorkers<WorkerT>::RunCallbackThread() {
  while (IsRunning()) {
    const CallbackJob callback_job = results_queue_.Dequeue();

    // stop callback processing if needed
    if (callback_job.is_death_sentence) {
      break;
    }

    // notify all observers about the finished job
    this->NotifyListeners(callback_job.data);

    // remove the job from the pending 'list' and add it to the ready 'list'
    atomic_dec32(&jobs_pending_);
    atomic_inc64(&jobs_processed_);

    // signal the Spooler that all jobs are done...
    if (atomic_read32(&jobs_pending_) == 0) {
      pthread_cond_broadcast(&jobs_all_done_);
    }
  }
}


template <class WorkerT>
void ConcurrentWorkers<WorkerT>::ReportStartedWorker() const {
  MutexLockGuard lock(status_mutex_);
  ++workers_started_;
  pthread_cond_signal(&worker_started_);
}


template <class WorkerT>
void ConcurrentWorkers<WorkerT>::Schedule(WorkerJob job) {
  // Note: This method can be called from arbitrary threads. Thus we do not
  //       necessarily have just one producer in the system.

  // check if it makes sense to schedule this job
  if (!IsRunning() && !job.is_death_sentence) {
    LogCvmfs(kLogConcurrency, kLogWarning, "Tried to schedule a job but "
                                           "concurrency was not running...");
    return;
  }

  jobs_queue_.Enqueue(job);
  if (!job.is_death_sentence) {
    atomic_inc32(&jobs_pending_);
  }
}


template <class WorkerT>
void ConcurrentWorkers<WorkerT>::ScheduleDeathSentences() {
  assert (!IsRunning());

  // make sure that the queue is empty before we schedule a death sentence
  TruncateJobQueue();
  EnableDrainoutMode();

  // schedule a death sentence for each running thread
  const unsigned int number_of_workers = GetNumberOfWorkers();
  for (unsigned int i = 0; i < number_of_workers; ++i) {
    Schedule(WorkerJob());
  }

  // schedule a death sentence for the callback thread
  results_queue_.Enqueue(CallbackJob());
}


template <class WorkerT>
typename ConcurrentWorkers<WorkerT>::WorkerJob
  ConcurrentWorkers<WorkerT>::Acquire()
{
  // Note: This method is exclusively called inside the worker threads!
  //       Any other usage might produce undefined behavior.
  return jobs_queue_.Dequeue();
}


template <class WorkerT>
void ConcurrentWorkers<WorkerT>::TruncateJobQueue(const bool forget_pending) {
  // Note: This method will throw away all jobs currently waiting in the job
  //       queue. These jobs will not be processed!
  const unsigned int dropped_jobs = jobs_queue_.Drop();

  // if desired, we remove the jobs from the pending 'list'
  if (forget_pending) {
    atomic_xadd32(&jobs_pending_, -dropped_jobs);
  }
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

  // wait for the callback worker thread
  pthread_join(callback_thread_, NULL);

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

  // wait until all pending jobs are processed
  EnableDrainoutMode();
  {
    MutexLockGuard lock(jobs_all_done_mutex_);
    while (atomic_read32(&jobs_pending_) > 0) {
      pthread_cond_wait(&jobs_all_done_, &jobs_all_done_mutex_);
    }
  }
  DisableDrainoutMode();

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

  // queue the result in the callback channel
  results_queue_.Enqueue(CallbackJob(data));
}


#ifdef CVMFS_NAMESPACE_GUARD
}
#endif
