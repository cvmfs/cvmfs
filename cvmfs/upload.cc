#include "upload.h"

#include <vector>

#include "upload_local.h"
#include "upload_riak.h"

using namespace upload;

Spooler::SpoolerDefinition::SpoolerDefinition(
                                    const std::string& definition_string) :
  valid_(false)
{
  // split the spooler definition into spooler driver and pipe definitions
  std::vector<std::string> components = SplitString(definition_string, ',');
  if (components.size() != 3) {
    LogCvmfs(kLogSpooler, kLogStderr, "Invalid spooler definition");
    return;
  }

  // split the spooler driver definition into name and config part
  std::vector<std::string> upstream = SplitString(components[0], ':', 2);
  if (upstream.size() != 2) {
    LogCvmfs(kLogSpooler, kLogStderr, "Invalid spooler driver");
    return;
  }

  // recognize and configure the spooler driver
  if (upstream[0] == "local") {
    driver_type   = Local;
  } else if (upstream[0] == "riak") {
    driver_type   = Riak;
  } else {
    LogCvmfs(kLogSpooler, kLogStderr, "unknown spooler driver: %s",
      upstream[0].c_str());
    return;
  }
  spooler_description = upstream[1];

  // save named pipe paths and validate this SpoolerDefinition
  paths_out_pipe  = components[1];
  digests_in_pipe = components[2];
  valid_ = true;
}

Spooler* Spooler::Construct(const std::string &spooler_description,
                            const int          max_pending_jobs) {
  SpoolerDefinition spooler_definition(spooler_description);
  Spooler *spooler = NULL;

  // I know that this is crap! Refactoring in baby steps
  // THIS IS A CRUTCH
  switch (spooler_definition.driver_type) {
    case SpoolerDefinition::Local:
      spooler = new SpoolerImpl<LocalPushWorker>(spooler_description, max_pending_jobs);
      break;

    case SpoolerDefinition::Riak:
      spooler = new SpoolerImpl<RiakPushWorker>(spooler_description, max_pending_jobs);
      break;

    default:
      LogCvmfs(kLogSpooler, kLogStderr, "invalid spooler definition");
  }

  assert (spooler != NULL);

  if (!spooler->Initialize()) {
    delete spooler;
    return NULL;
  }

  return spooler;
}


Spooler::Spooler(const std::string &spooler_description,
                 const int          max_pending_jobs) :
  callback_(NULL),
  job_queue_max_length_(max_pending_jobs),
  spooler_description_(spooler_description),
  transaction_ends_(false),
  initialized_(false),
  move_(false)
{
  atomic_init32(&jobs_pending_);
  atomic_init32(&jobs_failed_);
}


bool Spooler::Initialize() {
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "Initializing Spooler backend");

  // initialize synchronisation for job queue (PushWorkers)
  if (pthread_mutex_init(&job_queue_mutex_, NULL) != 0)         return false;
  if (pthread_cond_init(&job_queue_cond_not_full_, NULL) != 0)  return false;
  if (pthread_cond_init(&job_queue_cond_not_empty_, NULL) != 0) return false;

  // spawn the PushWorker objects in their own threads
  if (!SpawnPushWorkers()) {
    LogCvmfs(kLogSpooler, kLogWarning, "Failed to spawn concurrent push workers");
    return false;
  }

  initialized_ = true;
  return true;
}


Spooler::~Spooler() {
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "Spooler backend terminates");

  // clean the mess the PushWorkers produced
  // TearDown();
  // TODO! not possible here, since we have no polymorphy in destructor

  // free PushWorker synchronisation primitives
  pthread_cond_destroy(&job_queue_cond_not_full_);
  pthread_cond_destroy(&job_queue_cond_not_empty_);
  pthread_mutex_destroy(&job_queue_mutex_);
}


void Spooler::Copy(const std::string &local_path,
                   const std::string &remote_path) {
  LogCvmfs(kLogSpooler, kLogVerboseMsg,
           "Spooler received 'copy': source %s, dest %s move %d",
           local_path.c_str(), remote_path.c_str(), move_);

  Schedule(new StorageCopyJob(local_path, remote_path, move_, this));
}


void Spooler::Process(const std::string &local_path,
                      const std::string &remote_dir,
                      const std::string &file_suffix) {
  LogCvmfs(kLogSpooler, kLogVerboseMsg,
           "Spooler received 'process': source %s, dest %s, "
           "postfix %s, move %d", local_path.c_str(),
           remote_dir.c_str(), file_suffix.c_str(), move_);

  Schedule(new StorageCompressionJob(local_path, remote_dir, file_suffix, move_, this));
}


void Spooler::EndOfTransaction() {
  assert(!transaction_ends_);

  LogCvmfs(kLogSpooler, kLogVerboseMsg,
           "Spooler received 'end of transaction'");

  // Schedule a death sentence for every running worker thread.
  // Since we have a FIFO queue the death sentences will be at the end of the
  // line waiting for the threads to kill them.
  const int number_of_threads = GetNumberOfWorkers();
  for (int i = 0; i < number_of_threads; ++i) {
    Schedule(new DeathSentenceJob(this));
  }

  transaction_ends_ = true;
}


void Spooler::Schedule(Job *job) {
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "scheduling new job into job queue: %s",
           job->name().c_str());

  pthread_mutex_lock(&job_queue_mutex_);

  // wait until there is space in the job queue
  while (job_queue_.size() >= job_queue_max_length_) {
    pthread_cond_wait(&job_queue_cond_not_full_, &job_queue_mutex_);
  }

  // put something into the job queue
  job_queue_.push(job);
  atomic_inc32(&jobs_pending_);

  // wake all waiting threads
  pthread_cond_broadcast(&job_queue_cond_not_empty_);

  pthread_mutex_unlock(&job_queue_mutex_);
}


Job* Spooler::AcquireJob() {
  pthread_mutex_lock(&job_queue_mutex_);

  // wait until there is something to do
  while (job_queue_.empty()) {
    pthread_cond_wait(&job_queue_cond_not_empty_, &job_queue_mutex_);
  }

  // get the job and remove it from the queue
  Job* job = job_queue_.front();
  job_queue_.pop();

  // signal the Spooler that there is at least one free space now
  if (job_queue_.size() < job_queue_max_length_) {
    pthread_cond_signal(&job_queue_cond_not_full_);
  }

  pthread_mutex_unlock(&job_queue_mutex_);

  // return the acquired job
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "acquired a job from the job queue: %s",
           job->name().c_str());
  return job;
}


void Spooler::JobFinishedCallback(Job* job) {
  // BEWARE!
  // This callback might be called from a different thread!

  if (!job->IsSuccessful()) {
    atomic_inc32(&jobs_failed_);
    LogCvmfs(kLogSpooler, kLogWarning, "Spooler Job '%s' failed.",
             job->name().c_str());
  } else {
    LogCvmfs(kLogSpooler, kLogVerboseMsg, "Spooler Job '%s' succeeded.",
             job->name().c_str());
  }

  InvokeExternalCallback(job);

  delete job;
  atomic_dec32(&jobs_pending_);
}


void Spooler::InvokeExternalCallback(Job* job) {
  if (NULL == callback_)
    return;

  if (job->IsCompressionJob()) {
    StorageCompressionJob *compression_job =
      dynamic_cast<StorageCompressionJob*>(job);
    (*callback_)(compression_job->local_path(),
                 compression_job->return_code(),
                 compression_job->content_hash());
  } else

  if (job->IsCopyJob()) {
    StorageCopyJob *copy_job =
      dynamic_cast<StorageCopyJob*>(job);
    (*callback_)(copy_job->local_path(),
                 copy_job->return_code());
  }
}


void Spooler::SetCallback(SpoolerCallbackBase *callback_object) {
  assert (callback_ == NULL);
  callback_ = callback_object;
}


void Spooler::UnsetCallback() {
  delete callback_;
  callback_ = NULL;
}


// -----------------------------------------------------------------------------

bool LocalStat::Stat(const std::string &path) {
  return FileExists(base_path_ + "/" + path);
}

namespace upload {

  BackendStat *GetBackendStat(const std::string &spooler_definition) {
    std::vector<std::string> components = SplitString(spooler_definition, ',');
    std::vector<std::string> upstream = SplitString(components[0], ':');
    if ((upstream.size() != 2) || (upstream[0] != "local")) {
      PrintError("Invalid upstream");
      return NULL;
    }
    return new LocalStat(upstream[1]);
  }
  
}
