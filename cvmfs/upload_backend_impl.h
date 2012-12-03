#include <fcntl.h>
#include <errno.h>

#include "compression.h"
#include "logging.h"
#include "util.h"

namespace upload {


template <class PushWorkerT>
SpoolerBackend<PushWorkerT>::SpoolerBackend(const std::string &spooler_description) :
  job_queue_max_length_(50), // TODO: make this configurable
  spooler_description_(spooler_description),
  pipes_connected_(false),
  initialized_(false) {}


template <class PushWorkerT>
SpoolerBackend<PushWorkerT>::~SpoolerBackend() {
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "Spooler backend terminates");

  // close pipe connection
  fclose(fpathes_);
  close(fd_digests_);

  // free PushWorker synchronisation primitives
  pthread_cond_destroy(&job_queue_cond_not_full_);
  pthread_cond_destroy(&job_queue_cond_not_empty_);
  pthread_mutex_destroy(&job_queue_mutex_);
}


template <class PushWorkerT>
bool SpoolerBackend<PushWorkerT>::Connect(const std::string &fifo_paths,
                                          const std::string &fifo_digests) {
  // connect to incoming pathes pipe
  fpathes_ = fopen(fifo_paths.c_str(), "r");
  if (!fpathes_) {
    LogCvmfs(kLogSpooler, kLogStderr,
             "Spooler Backend failed to connect to pathes pipe");
    return false;
  }
  LogCvmfs(kLogSpooler, kLogVerboseMsg,
           "Spooler Backend connected to pathes pipe");
  
  // open a pipe for outgoing digest hashes
  fd_digests_ = open(fifo_digests.c_str(), O_WRONLY);
  if (fd_digests_ < 0) {
    fclose(fpathes_);
    LogCvmfs(kLogSpooler, kLogStderr,
             "Spooler Backend failed to create digests pipe");
    return false;
  }
  LogCvmfs(kLogSpooler, kLogVerboseMsg,
           "Spooler Backend created digests pipe");

  // all set and ready to go...
  pipes_connected_ = true;
  return true;
}


template <class PushWorkerT>
bool SpoolerBackend<PushWorkerT>::Initialize() {
  // check if pipes are connected properly
  if (!pipes_connected_) {
    LogCvmfs(kLogSpooler, kLogWarning, "IO pipes are not setup properly");
    return false;
  }

  // initialize synchronisation for job queue (PushWorkers)
  pthread_mutex_init(&job_queue_mutex_, NULL);
  pthread_cond_init(&job_queue_cond_not_full_, NULL);
  pthread_cond_init(&job_queue_cond_not_empty_, NULL);

  // spawn the PushWorker objects in their own threads
  if (!SpawnPushWorkers()) {
    LogCvmfs(kLogSpooler, kLogWarning, "Failed to spawn concurrent push workers");
    return false;
  }

  initialized_ = true;
  return true;
}


template <class PushWorkerT>
bool SpoolerBackend<PushWorkerT>::SpawnPushWorkers() {
  // find out about the environment of our PushWorker swarm
  pushworker_context_  = PushWorkerT::GenerateContext(this, spooler_description_);
  int workers_to_spawn = PushWorkerT::GetNumberOfWorkers(pushworker_context_);

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
                                      &SpoolerBackend::RunPushWorker,
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
void* SpoolerBackend<PushWorkerT>::RunPushWorker(void* context) {
  // get the context object pointer out of the void* pointer
  typename PushWorkerT::Context *ctx =
    static_cast<typename PushWorkerT::Context*>(context);

  // boot up the push worker object and make sure it works
  PushWorkerT worker(ctx);
  worker.Initialize();
  if (!worker.IsReady())
    return NULL;
  SpoolerBackend<PushWorkerT> *master = ctx->master;

  // start the processing loop
  while (true) {
    Job *job = master->AcquireJob();
    if (job->IsDeathSentenceJob()) {
      delete job;
      break;
    }

    assert (job->IsStorageJob());
    worker.ProcessJob(dynamic_cast<StorageJob*>(job));
    delete job;
  }

  // good bye thread...
  return context;
}


template <class PushWorkerT>
int SpoolerBackend<PushWorkerT>::Run() {
  int retval;
  bool running = true;

  // reading from input pipe until it is gone
  while ((retval = getc_unlocked(fpathes_)) != EOF && running) {

    // figure out if file should be moved and what the actual command is
    bool move_file = false;
    if (retval & kCmdMoveFlag) {
      retval -= kCmdMoveFlag;
      move_file = true;
    }
    unsigned char command = retval;

    switch (command) {
      case kCmdEndOfTransaction:
        EndOfTransaction();
        running = false;
        break;

      case kCmdCopy:
        Copy(move_file);
        break;

      case kCmdProcess:
        Process(move_file);
        break;

      default:
        Unknown(command);
        break;
    }
  }

  return 0;
}


template <class PushWorkerT>
void SpoolerBackend<PushWorkerT>::SendResult(
                                    const int error_code,
                                    const std::string &local_path,
                                    const hash::Any &compressed_hash) const {
  // create response message
  std::string response;
  response = StringifyInt(error_code);
  response.push_back('\0');
  response.append(local_path);
  response.push_back('\0');
  response.append((compressed_hash.IsNull()) ? "" : compressed_hash.ToString());
  response.push_back('\n');

  // send message to Spooler Frontend
  LogCvmfs(kLogSpooler, kLogVerboseMsg,
           "Spooler sends back result: %s",
           response.c_str());

  WritePipe(fd_digests_, response.data(), response.length());
}


template <class PushWorkerT>
void SpoolerBackend<PushWorkerT>::Schedule(Job *job) {
  pthread_mutex_lock(&job_queue_mutex_);

  // wait until there is space in the job queue
  while (job_queue_.size() >= job_queue_max_length_) {
    pthread_cond_wait(&job_queue_cond_not_full_, &job_queue_mutex_);
  }

  // put something into the job queue
  job_queue_.push(job);

  // wake all waiting threads
  pthread_cond_broadcast(&job_queue_cond_not_empty_);

  pthread_mutex_unlock(&job_queue_mutex_);
}


template <class PushWorkerT>
Job* SpoolerBackend<PushWorkerT>::AcquireJob() {
  pthread_mutex_lock(&job_queue_mutex_);

  // wait until there is something to do
  while (job_queue_.empty()) {
    pthread_cond_wait(&job_queue_cond_not_empty_, &job_queue_mutex_);
  }

  // get the job and remove it from the queue
  Job* job = job_queue_.front();
  job_queue_.pop();

  // signal the SpoolerBackend that there is at least one free space now
  if (job_queue_.size() < job_queue_max_length_) {
    pthread_cond_signal(&job_queue_cond_not_full_);
  }

  pthread_mutex_unlock(&job_queue_mutex_);

  // return the acquired job
  return job;
}


template <class PushWorkerT>
void SpoolerBackend<PushWorkerT>::EndOfTransaction() {
  LogCvmfs(kLogSpooler, kLogVerboseMsg,
           "Spooler received 'end of transaction'");

  SendResult(0);
}


template <class PushWorkerT>
void SpoolerBackend<PushWorkerT>::Copy(const bool move) {
  std::string local_path;
  std::string remote_path;

  GetString(fpathes_, &local_path);
  GetString(fpathes_, &remote_path);
  LogCvmfs(kLogSpooler, kLogVerboseMsg,
           "Spooler received 'copy': source %s, dest %s move %d",
           local_path.c_str(), remote_path.c_str(), move);

  StorageJob *job = new StorageCopyJob(local_path, remote_path, move);
  Schedule(job);
}


template <class PushWorkerT>
void SpoolerBackend<PushWorkerT>::Process(const bool move) {
  std::string local_path;
  std::string remote_dir;
  std::string file_suffix;

  GetString(fpathes_, &local_path);
  GetString(fpathes_, &remote_dir);
  GetString(fpathes_, &file_suffix);
  LogCvmfs(kLogSpooler, kLogVerboseMsg,
           "Spooler received 'process': source %s, dest %s, "
           "postfix %s, move %d", local_path.c_str(),
           remote_dir.c_str(), file_suffix.c_str(), move);

  StorageJob *job = new StorageCompressionJob(local_path, remote_dir, file_suffix, move);
  Schedule(job);
}


template <class PushWorkerT>
void SpoolerBackend<PushWorkerT>::Unknown(const unsigned char command) {
  LogCvmfs(kLogSpooler, kLogWarning, "Spooler received 'unknown command': %d",
           command);

  SendResult(1);
}


template <class PushWorkerT>
bool SpoolerBackend<PushWorkerT>::IsReady() const {
  return 
    pipes_connected_ &&
    initialized_;
}


template <class PushWorkerT>
bool SpoolerBackend<PushWorkerT>::GetString(FILE *f, std::string *str) const {
  str->clear();
  do {
    int retval = getc_unlocked(f);
    if (retval == EOF)
      return false;
    char c = retval;
    if (c == '\0')
      return true;
    str->push_back(c);
  } while (true);
}


} // namespace upload

// -----------------------------------------------------------------------------


// bool SpoolerBackend<PushWorkerT>::StoragePushJob::Compress(CompressionContext *ctx) {
//   // Create a temporary file at the given destination directory
//   FILE *fcas = CreateTempFile(ctx->tmp_dir + "/cvmfs", 0777, "w",
//                               &compressed_file_path_);
//   if (fcas == NULL) {
//     LogCvmfs(kLogSpooler, kLogStderr, "failed to create temporary file %s",
//              compressed_file_path_.c_str());
//     return false;
//   }

//   // Compress the provided source file and write the result into the temporary.
//   // Additionally computes the content hash of the compressed data
//   int retval = zlib::CompressPath2File(local_path_, fcas, &content_hash_);
//   if (! retval) {
//     LogCvmfs(kLogSpooler, kLogStderr, "failed to compress file %s to temporary "
//                                       "file %s",
//              local_path_.c_str(), compressed_file_path_.c_str());

//     unlink(compressed_file_path_.c_str());
//     return false;
//   }
//   fclose(fcas);

//   return true;
// }


// bool SpoolerBackend<PushWorkerT>::StoragePushJob::PushToStorage(StoragePushContext *ctx) {
//   const std::string remote_path = remote_dir_                  +
//                                   content_hash_.MakePath(1, 2) +
//                                   file_suffix_;

//   return delegate_->PushToStorage(ctx, compressed_file_path_, remote_path);
// }
