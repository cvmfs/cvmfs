/**
 * This file is part of the CernVM File System.
 */

#include <fcntl.h>
#include <errno.h>

#include "compression.h"
#include "logging.h"
#include "util.h"

#include "upload_jobs.h"

namespace upload {

template <class PushWorkerT>
bool SpoolerImpl<PushWorkerT>::SpawnPushWorkers() {
  // do some global initialization work common for all PushWorkers
  const bool retval = PushWorkerT::DoGlobalInitialization();
  if (!retval) {
    LogCvmfs(kLogSpooler, kLogWarning, "Failed to globally initialize "
                                       "PushWorkers");
    return false;
  }

  // find out about the environment of our PushWorker swarm
  pushworker_context_  = PushWorkerT::GenerateContext(this, spooler_definition());
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
                                      &SpoolerImpl::RunPushWorker,
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
void* SpoolerImpl<PushWorkerT>::RunPushWorker(void* context) {
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
  SpoolerImpl<PushWorkerT> *master = ctx->master;

  //
  // PROCESSING LOOP
  //////////////////

  // start the processing loop
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "Running PushWorker...");
  while (true) {
    // check if we should stop working and die silently
    Job *job = master->AcquireJob();
    if (job->IsDeathSentenceJob()) {
      job->Finished();
      break;
    }

    // do the job we were asked for
    assert (job->IsStorageJob());
    StorageJob *storage_job = dynamic_cast<StorageJob*>(job);
    worker.ProcessJob(storage_job);
  }

  //
  // TEAR DOWN
  ////////////

  // good bye thread...
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "Terminating PushWorker thread...");
  return context;
}


template <class PushWorkerT>
int SpoolerImpl<PushWorkerT>::GetNumberOfWorkers() const {
  return pushworker_threads_.size();
}

template <class PushWorkerT>
void SpoolerImpl<PushWorkerT>::WaitForTermination() const {
  assert (TransactionEnded());

  LogCvmfs(kLogSpooler, kLogVerboseMsg, "Waiting for Jobs to finish and PushWorkers to terminate.");
  // wait for all running worker threads to terminate
  WorkerThreads::const_iterator i = pushworker_threads_.begin();
  WorkerThreads::const_iterator iend = pushworker_threads_.end();
  for (; i != iend; ++i) {
    pthread_join(*i, NULL);
  }

  LogCvmfs(kLogSpooler, kLogVerboseMsg, "Spooler stopped working...");
}

template <class PushWorkerT>
void SpoolerImpl<PushWorkerT>::TearDown() {
  PushWorkerT::DoGlobalCleanup();
}


} // namespace upload
