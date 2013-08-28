/**
 * This file is part of the CernVM File System.
 */

#include "upload.h"

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
  file_processor_ = new FileProcessor(spooler_definition_.use_file_chunking,
                                      spooler_definition_.min_file_chunk_size,
                                      spooler_definition_.avg_file_chunk_size,
                                      spooler_definition_.max_file_chunk_size);
  //file_processor_->RegisterListener(&Spooler::ProcessingCallback, this);

  // all done...
  return true;
}


void Spooler::TearDown() {
  WaitForTermination();
}


void Spooler::Process(const std::string &local_path,
                      const bool         allow_chunking) {
  file_processor_->Process(local_path, allow_chunking);
}


void Spooler::ProcessCatalog(const std::string &local_path) {
  file_processor_->Process(local_path, false, "C");
}


void Spooler::Upload(const std::string &local_path,
                     const std::string &remote_path) {
  uploader_->Upload(local_path,
                    remote_path,
                    AbstractUploader::MakeCallback(&Spooler::UploadingCallback,
                                                   this));
}


bool Spooler::Remove(const std::string &file_to_delete) {
  return uploader_->Remove(file_to_delete);
}


bool Spooler::Peek(const std::string &path) const {
  return uploader_->Peek(path);
}


void Spooler::ProcessingCallback(const SpoolerResult &data) {
  NotifyListeners(data);
}


void Spooler::UploadingCallback(const UploaderResults &data) {
  NotifyListeners(SpoolerResult(data.return_code,
                                data.local_path));
}


void Spooler::WaitForUpload() const {
  uploader_->WaitForUpload();
  file_processor_->WaitForProcessing();
}


void Spooler::WaitForTermination() const {
  uploader_->WaitForUpload();
  file_processor_->WaitForProcessing();
}


unsigned int Spooler::GetNumberOfErrors() const {
  return uploader_->GetNumberOfErrors();
}
