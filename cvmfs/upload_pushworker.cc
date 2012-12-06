#include "upload_pushworker.h"

#include <unistd.h>

using namespace upload;

const int AbstractPushWorker::default_number_of_processors = 1;


bool AbstractPushWorker::Initialize() {
  return true;
}


bool AbstractPushWorker::IsReady() const {
  return true;
}


bool AbstractPushWorker::DoGlobalInitialization() {
  return true;
}


void AbstractPushWorker::DoGlobalCleanup() {}


bool AbstractPushWorker::ProcessJob(StorageJob *job) {
  if (job->IsCompressionJob()) {
    StorageCompressionJob 
    *compression_job = dynamic_cast<StorageCompressionJob*>(job);
    ProcessCompressionJob(compression_job);
  } else if (job->IsCopyJob()) {
    StorageCopyJob *copy_job = dynamic_cast<StorageCopyJob*>(job);
    ProcessCopyJob(copy_job);
  }

  return job->IsSuccessful();
}


int AbstractPushWorker::GetNumberOfCpuCores() {
  const int numCPU = sysconf(_SC_NPROCESSORS_ONLN);

  if (numCPU <= 0) {
    LogCvmfs(kLogSpooler, kLogWarning, "Unable to determine the available "
                                       "number of processors in the system... "
                                       "falling back to default '%d'",
             AbstractPushWorker::default_number_of_processors);
    return AbstractPushWorker::default_number_of_processors;
  }

  return numCPU;
}
