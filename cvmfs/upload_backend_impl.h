#include <fcntl.h>
#include <errno.h>

#include "compression.h"
#include "logging.h"
#include "util.h"

namespace upload {

template <class PushWorkerT>
bool SpoolerBackendImpl<PushWorkerT>::SpawnPushWorkers() {
  // do some global initialization work common for all PushWorkers
  const bool retval = PushWorkerT::DoGlobalInitialization();
  if (!retval) {
    LogCvmfs(kLogSpooler, kLogWarning, "Failed to globally initialize "
                                       "PushWorkers");
    return false;
  }

  // find out about the environment of our PushWorker swarm
  pushworker_context_  = PushWorkerT::GenerateContext(this, spooler_description());
  int workers_to_spawn = PushWorkerT::GetNumberOfWorkers(pushworker_context_);

  LogCvmfs(kLogSpooler, kLogVerboseMsg, "Using %d concurrent publishing workers",
           workers_to_spawn);

  // initialize the PushWorker thread pool
  assert (pushworker_threads_.size() == 0);
  pushworker_threads_.resize(workers_to_spawn);

  // spawn the swarm and make them work
  bool success = true;
  WorkerThreads::iterator i          = pushworker_threads_.begin();
  WorkerThreads::const_iterator iend = pushworker_threads_.end();
  for (; i != iend; ++i) {
    pthread_t* thread = &(*i);
    const int retval = pthread_create(thread, 
                                      NULL,
                                      &SpoolerBackendImpl::RunPushWorker,
                                      pushworker_context_);
    if (retval != 0) {
      LogCvmfs(kLogSpooler, kLogWarning, "Failed to spawn a PushWorker.");
      success = false;
    }
  }

  // all done...
  return success;
}


template <class PushWorkerT>
void* SpoolerBackendImpl<PushWorkerT>::RunPushWorker(void* context) {
  //
  // INITIALIZATION
  /////////////////

  LogCvmfs(kLogSpooler, kLogVerboseMsg, "Starting PushWorker thread...");
  // get the context object pointer out of the void* pointer
  typename PushWorkerT::Context *ctx =
    static_cast<typename PushWorkerT::Context*>(context);

  // boot up the push worker object and make sure it works
  PushWorkerT worker(ctx);
  worker.Initialize();
  if (!worker.IsReady()) {
    LogCvmfs(kLogSpooler, kLogWarning, "Push Worker was not initialized "
                                       "properly... will die now!");
    return NULL; 
  }
  SpoolerBackendImpl<PushWorkerT> *master = ctx->master;

  // find out about the thread number
  ctx->Lock();
  const int thread_number = ctx->base_thread_number++;
  ctx->Unlock();

  //
  // PROCESSING LOOP
  //////////////////

  // start the processing loop
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "Running PushWorker...");
  while (true) {
    // check if we should stop working and die silently
    Job *job = master->AcquireJob();
    if (job->IsDeathSentenceJob()) {
      delete job;
      break;
    }

    // do the job we were asked for
    assert (job->IsStorageJob());
    StorageJob *storage_job = dynamic_cast<StorageJob*>(job);
    worker.ProcessJob(storage_job);

    // report to the supervisor and get ready for the next work piece
    if (!storage_job->IsSuccessful()) {
      LogCvmfs(kLogSpooler, kLogWarning, "Job '%s' failed in Thread %d",
               storage_job->name().c_str(), thread_number);
    } else {
      LogCvmfs(kLogSpooler, kLogVerboseMsg, "Job '%s' succeeded in Thread %d",
               storage_job->name().c_str(), thread_number);
    }
    delete job;
  }

  //
  // TEAR DOWN
  ////////////

  // good bye thread...
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "Terminating PushWorker thread...");
  return context;
}


template <class PushWorkerT>
int SpoolerBackendImpl<PushWorkerT>::GetNumberOfWorkers() const {
  return pushworker_threads_.size();
}


} // namespace upload
