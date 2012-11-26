#include <fcntl.h>
#include <errno.h>

#include "compression.h"
#include "logging.h"
#include "util.h"

namespace upload {

template <class PushWorkerT>
SpoolerBackend<PushWorkerT>::SpoolerBackend(const std::string &spooler_description) :
  spooler_description_(spooler_description),
  pipes_connected_(false),
  initialized_(false)
{}


template <class PushWorkerT>
SpoolerBackend<PushWorkerT>::~SpoolerBackend() {
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "Spooler backend terminates");
  fclose(fpathes_);
  close(fd_digests_);
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
  if (!pipes_connected_) {
    LogCvmfs(kLogSpooler, kLogWarning, "IO pipes are not setup properly");
    return false;
  }

  initialized_ = true;
  return true;
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
void SpoolerBackend<PushWorkerT>::Schedule(StorageJob *job) {
  
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
