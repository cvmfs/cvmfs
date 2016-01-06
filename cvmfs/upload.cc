/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "upload.h"

#include <vector>

#include "util_concurrency.h"

namespace upload {

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


Spooler::~Spooler() {
  if (uploader_) {
    uploader_->TearDown();
  }
}


bool Spooler::Initialize() {
  // configure the uploader environment
  uploader_ = AbstractUploader::Construct(spooler_definition_);
  if (!uploader_) {
    LogCvmfs(kLogSpooler, kLogWarning, "Failed to initialize backend upload "
                                       "facility in Spooler.");
    return false;
  }

  // configure the file processor context
  file_processor_ = new FileProcessor(uploader_.weak_ref(),
                                      spooler_definition_);
  file_processor_->RegisterListener(&Spooler::ProcessingCallback, this);

  // all done...
  return true;
}


void Spooler::Process(const std::string &local_path,
                      const bool         allow_chunking) {
  file_processor_->Process(local_path, allow_chunking);
}


void Spooler::ProcessCatalog(const std::string &local_path) {
  file_processor_->Process(local_path, false, shash::kSuffixCatalog);
}

void Spooler::ProcessHistory(const std::string &local_path) {
  file_processor_->Process(local_path, false, shash::kSuffixHistory);
}

void Spooler::ProcessCertificate(const std::string &local_path) {
  file_processor_->Process(local_path, false, shash::kSuffixCertificate);
}

void Spooler::ProcessMetainfo(const std::string &local_path) {
  file_processor_->Process(local_path, false, shash::kSuffixMetainfo);
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


bool Spooler::PlaceBootstrappingShortcut(const shash::Any &object) const {
  assert(!object.IsNull());
  return uploader_->PlaceBootstrappingShortcut(object);
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


unsigned int Spooler::GetNumberOfErrors() const {
  return uploader_->GetNumberOfErrors();
}

}  // namespace upload
