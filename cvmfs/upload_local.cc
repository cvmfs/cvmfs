/**
 * This file is part of the CernVM File System.
 */

#include "upload_local.h"

#include <errno.h>

#include "logging.h"
#include "compression.h"
#include "util.h"

using namespace upload;


LocalSpooler::LocalSpooler(const SpoolerDefinition &spooler_definition) :
  AbstractSpooler(spooler_definition),
  upstream_path_(spooler_definition.spooler_description),
  concurrent_compression_(NULL),
  worker_context_(NULL)
{
  assert (spooler_definition.IsValid() &&
          spooler_definition.driver_type == SpoolerDefinition::Local);

  atomic_init32(&copy_errors_);
}

  
unsigned int LocalSpooler::GetNumberOfErrors() const {
  return concurrent_compression_->GetNumberOfFailedJobs() + atomic_read32(&copy_errors_);
}


bool LocalSpooler::Initialize() {
  const unsigned int number_of_cpus = GetNumberOfCpuCores();

  worker_context_ = new LocalCompressionWorker::worker_context(upstream_path_);

  concurrent_compression_ =
    new ConcurrentWorkers<LocalCompressionWorker>(number_of_cpus,
                                                  number_of_cpus * 10, // TODO: magic number (?)
                                                  worker_context_);

  assert (worker_context_ && concurrent_compression_);

  if (! concurrent_compression_->Initialize()) {
    return false;
  }

  concurrent_compression_->RegisterListener(&LocalSpooler::CompressionCallback,
                                             this);

  return true;
}


void LocalSpooler::TearDown() {
  concurrent_compression_->WaitForTermination();
}


void LocalSpooler::Process(const std::string &local_path,
                           const std::string &remote_dir,
                           const std::string &file_suffix) {
  LocalCompressionWorker::expected_data input(local_path,
                                              remote_dir,
                                              file_suffix,
                                              move());
  concurrent_compression_->Schedule(input);
}


void LocalSpooler::CompressionCallback(
                          const LocalCompressionWorker::returned_data &data) {
  NotifyListeners(data);
}


void LocalSpooler::Copy(const std::string &local_path,
                        const std::string &remote_path) {
  const std::string destination_path = upstream_path_ + "/" + remote_path;

  int retcode = 0;
  if (move()) {
    int retval = rename(local_path.c_str(), destination_path.c_str());
    retcode    = (retval == 0) ? 0 : errno;
  } else {
    int retval = CopyPath2Path(local_path, destination_path);
    retcode    = retval ? 0 : 100;
  }

  if (retcode != 0) {
    atomic_inc32(&copy_errors_);
  }

  const SpoolerResult result(retcode, local_path);
  NotifyListeners(result);
}


void LocalSpooler::EndOfTransaction() {
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "End of Transaction");
  WaitForTermination();
}


void LocalSpooler::WaitForUpload() const {
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "Wait for Upload");
  concurrent_compression_->WaitForEmptyQueue();
}


void LocalSpooler::WaitForTermination() const {
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "Wait for Termination");
  concurrent_compression_->WaitForTermination();
}


LocalSpooler::LocalCompressionWorker::LocalCompressionWorker(
                                              const worker_context *context) :
  upstream_path_(context->upstream_path) {}


void LocalSpooler::LocalCompressionWorker::operator()(
            const LocalSpooler::LocalCompressionWorker::expected_data &data) {
  hash::Any compressed_hash(hash::kSha1);

  const std::string &local_path  = data.local_path;
  const std::string &remote_dir  = data.remote_dir;
  const std::string &file_suffix = data.file_suffix;
  const bool         move        = data.move;

  const std::string remote_path = upstream_path_ + "/" +
                                  remote_dir;

  std::string tmp_path;
  FILE *fcas = CreateTempFile(remote_path + "/cvmfs", 0777, "w",
                              &tmp_path);

  int retcode = 0;
  if (fcas == NULL) {
    retcode = 103;
  } else {
    int retval = zlib::CompressPath2File(local_path,
                                         fcas,
                                        &compressed_hash);
    retcode = retval ? 0 : 103;
    fclose(fcas);
    if (retval) {
      const std::string cas_path = remote_path + compressed_hash.MakePath(1, 2)
                              + file_suffix;
      retval = rename(tmp_path.c_str(), cas_path.c_str());
      if (retval != 0) {
        unlink(tmp_path.c_str());
        retcode = 104;
      }
    }
  }
  if (move) {
    if (unlink(local_path.c_str()) != 0)
      retcode = 105;
  }

  returned_data return_values(retcode,
                              local_path,
                              compressed_hash);

  if (retcode == 0) {
    master()->JobSuccessful(return_values);
  } else {
    master()->JobFailed(return_values);
  }
}
