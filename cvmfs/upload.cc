/**
 * This file is part of the CernVM File System.
 */

#include "upload.h"
#include "cvmfs_config.h"

#include <vector>

#include "util_concurrency.h"

namespace upload {

Spooler *Spooler::Construct(const SpoolerDefinition &spooler_definition) {
  Spooler *result = new Spooler(spooler_definition);
  if (!result->Initialize()) {
    delete result;
    result = NULL;
  }
  return result;
}

Spooler::Spooler(const SpoolerDefinition &spooler_definition)
    : spooler_definition_(spooler_definition) {}

Spooler::~Spooler() {
  if (uploader_) {
    uploader_->TearDown();
  }
}

std::string Spooler::backend_name() const { return uploader_->name(); }

bool Spooler::Initialize() {
  // configure the uploader environment
  uploader_ = AbstractUploader::Construct(spooler_definition_);
  if (!uploader_) {
    LogCvmfs(kLogSpooler, kLogWarning,
             "Failed to initialize backend upload "
             "facility in Spooler.");
    return false;
  }

  // configure the file processor context
  ingestion_pipeline_ =
      new IngestionPipeline(uploader_.weak_ref(), spooler_definition_);
  ingestion_pipeline_->RegisterListener(&Spooler::ProcessingCallback, this);
  ingestion_pipeline_->Spawn();

  // all done...
  return true;
}

void Spooler::Process(const std::string &local_path,
                      const bool allow_chunking) {
  ingestion_pipeline_->Process(local_path, allow_chunking);
}

void Spooler::ProcessCatalog(const std::string &local_path) {
  ingestion_pipeline_->Process(local_path, false, shash::kSuffixCatalog);
}

void Spooler::ProcessHistory(const std::string &local_path) {
  ingestion_pipeline_->Process(local_path, false, shash::kSuffixHistory);
}

void Spooler::ProcessCertificate(const std::string &local_path) {
  ingestion_pipeline_->Process(local_path, false, shash::kSuffixCertificate);
}

void Spooler::ProcessMetainfo(const std::string &local_path) {
  ingestion_pipeline_->Process(local_path, false, shash::kSuffixMetainfo);
}

void Spooler::Upload(const std::string &local_path,
                     const std::string &remote_path) {
  uploader_->Upload(
      local_path, remote_path,
      AbstractUploader::MakeCallback(&Spooler::UploadingCallback, this));
}

void Spooler::UploadManifest(const std::string &local_path) {
  Upload(local_path, ".cvmfspublished");
}

void Spooler::UploadReflog(const std::string &local_path) {
  Upload(local_path, ".cvmfsreflog");
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
  NotifyListeners(SpoolerResult(data.return_code, data.local_path));
}

void Spooler::WaitForUpload() const {
  ingestion_pipeline_->WaitFor();
  uploader_->WaitForUpload();
}

bool Spooler::FinalizeSession(bool commit, const std::string &old_root_hash,
                              const std::string &new_root_hash,
                              const RepositoryTag &tag) const {
  return uploader_->FinalizeSession(commit, old_root_hash,
                                    new_root_hash, tag);
}

unsigned int Spooler::GetNumberOfErrors() const {
  return uploader_->GetNumberOfErrors();
}

}  // namespace upload
