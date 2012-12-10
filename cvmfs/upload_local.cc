/**
 * This file is part of the CernVM File System.
 */

#include "upload_local.h"

#include <errno.h>

#include "logging.h"
#include "compression.h"
#include "util.h"

using namespace upload;

LocalPushWorker::Context* LocalPushWorker::GenerateContext(
                              SpoolerImpl<LocalPushWorker>     *master,
                              const Spooler::SpoolerDefinition &spooler_definition) {
  assert (spooler_definition.IsValid() && 
          spooler_definition.driver_type == Spooler::SpoolerDefinition::Local);

  Context *ctx = new Context(master, spooler_definition.spooler_description);
  return ctx;
}

int LocalPushWorker::GetNumberOfWorkers(const Context *context) {
  return GetNumberOfCpuCores() * 3;
}


LocalPushWorker::LocalPushWorker(Context *context) :
  upstream_path_(context->upstream_path),
  initialized_(false)
{}


bool LocalPushWorker::Initialize() {
  bool retval = AbstractPushWorker::Initialize();
  if (!retval)
    return false;

  if (upstream_path_.empty()) {
    LogCvmfs(kLogSpooler, kLogWarning, "upstream path not configured");
    return false;
  }

  LogCvmfs(kLogSpooler, kLogVerboseMsg, "successfully initialized local "
                                        "spooler backend");

  initialized_ = true;
  return true;
}


void LocalPushWorker::ProcessCompressionJob(
                        StorageCompressionJob *compression_job) {
        hash::Any   &compressed_hash = compression_job->content_hash();
  const std::string &local_path      = compression_job->local_path();
  const std::string &remote_dir      = compression_job->remote_dir();
  const std::string &file_suffix     = compression_job->file_suffix();
  const bool         move            = compression_job->move();

  const std::string remote_path = upstream_path_ + "/" +
                                  remote_dir;

  std::string tmp_path;
  FILE *fcas = CreateTempFile(remote_path + "/cvmfs", 0777, "w",
                              &tmp_path);

  int retcode = 0;
  if (fcas == NULL) {
    retcode = 103;
  } else {
    int retval = zlib::CompressPath2File(compression_job->local_path(),
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

  compression_job->Finished(retcode);
}


void LocalPushWorker::ProcessCopyJob(StorageCopyJob *copy_job) {
  const std::string& local_path  = copy_job->local_path();
  const std::string& remote_path = copy_job->remote_path();
  const bool         move        = copy_job->move();

  const std::string destination_path = upstream_path_ + "/" + remote_path;

  int retcode = 0;
  if (move) {
    int retval = rename(local_path.c_str(), destination_path.c_str());
    retcode    = (retval == 0) ? 0 : errno;
  } else {
    int retval = CopyPath2Path(local_path, destination_path);
    retcode    = retval ? 0 : 100;
  }

  copy_job->Finished(retcode);
}


bool LocalPushWorker::IsReady() const {
  const bool ready = AbstractPushWorker::IsReady();
  return ready && initialized_;
}
