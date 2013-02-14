/**
 * This file is part of the CernVM File System.
 */

#include "upload.h"
#include "upload_file_chunker.h"

#include "util_concurrency.h"

#include <vector>

using namespace upload;


Spooler* Spooler::Construct(const SpoolerDefinition &spooler_definition) {
  Spooler *result = new Spooler(spooler_definition);
  if (!result->Initialize()) {
    delete result;
    result = NULL;
  }
  return result;
}


Spooler::Spooler(const SpoolerDefinition &spooler_definition) :
  spooler_definition_(spooler_definition)
{}


Spooler::~Spooler() {}


bool Spooler::Initialize() {
  // configure the uploader environment
  uploader_ = AbstractUploader::Construct(spooler_definition_);
  if (!uploader_) {
    LogCvmfs(kLogSpooler, kLogWarning, "Failed to initialize backend upload "
                                       "facility in Spooler.");
    return false;
  }

  // configure the file processor context
  concurrent_processing_context_ =
    new FileProcessor::worker_context(spooler_definition_.temporary_path,
                                      spooler_definition_.use_file_chunking,
                                      uploader_.weak_ref());

  // create and configure a file processor worker environment
  const unsigned int number_of_cpus = GetNumberOfCpuCores();
  concurrent_processing_ =
     new ConcurrentWorkers<FileProcessor>(number_of_cpus,
                                          number_of_cpus * 500, // TODO: magic number (?)
                                          concurrent_processing_context_.weak_ref());
  assert(concurrent_processing_);
  concurrent_processing_->RegisterListener(&Spooler::ProcessingCallback, this);

  // initialize the file processor environment
  if (! concurrent_processing_->Initialize()) {
    LogCvmfs(kLogSpooler, kLogWarning, "Failed to initialize concurrent "
                                       "processing in Spooler.");
    return false;
  }

  // configure the file chunking size restrictions
  if (spooler_definition_.use_file_chunking) {
    ChunkGenerator::SetFileChunkRestrictions(
          spooler_definition_.min_file_chunk_size,
          spooler_definition_.avg_file_chunk_size,
          spooler_definition_.max_file_chunk_size);
  }

  // all done...
  return true;
}


void Spooler::TearDown() {
  concurrent_processing_->WaitForTermination();
  uploader_->WaitForUpload();
}


void Spooler::Process(const std::string &local_path,
                      const bool         allow_chunking) {
  // fill the file processor parameter structure and schedule the job
  const FileProcessor::Parameters params(local_path, allow_chunking);
  concurrent_processing_->Schedule(params);
}


void Spooler::Upload(const std::string &local_path,
                     const std::string &remote_path) {
  uploader_->Upload(local_path,
                    remote_path,
                    AbstractUploader::MakeCallback(&Spooler::UploadingCallback, this));
}


bool Spooler::Peek(const std::string &path) const {
  return uploader_->Peek(path);
}

void Spooler::ProcessingCallback(const FileProcessor::Results &data) {
  NotifyListeners(SpoolerResult(data.return_code,
                                data.local_path,
                                data.bulk_file.content_hash(),
                                data.file_chunks));
}

void Spooler::UploadingCallback(const UploaderResults &data) {
  NotifyListeners(SpoolerResult(data.return_code,
                                data.local_path));
}


void Spooler::WaitForUpload() const {
  concurrent_processing_->WaitForEmptyQueue();
  uploader_->WaitForUpload();
}


void Spooler::WaitForTermination() const {
  concurrent_processing_->WaitForTermination();
  uploader_->WaitForUpload();
}


unsigned int Spooler::GetNumberOfErrors() const {
  return concurrent_processing_->GetNumberOfFailedJobs() +
         uploader_->GetNumberOfErrors();
}
