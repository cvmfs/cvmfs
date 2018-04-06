/**
 * This file is part of the CernVM File System.
 */

#include "upload.h"
#include "cvmfs_config.h"

#include <vector>

#include "util/shared_ptr.h"
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

void Spooler::Process(IngestionSource* source,
                      const bool allow_chunking) {
  ingestion_pipeline_->Process(source, allow_chunking);
}

void Spooler::ProcessCatalog(IngestionSource *catalog_source) {
  ingestion_pipeline_->Process(catalog_source, false, shash::kSuffixCatalog);
}

void Spooler::ProcessHistory(IngestionSource* history_source) {
  ingestion_pipeline_->Process(history_source, false, shash::kSuffixHistory);
}

void Spooler::ProcessCertificate(IngestionSource* certificate_source) {
  ingestion_pipeline_->Process(certificate_source, false,
                               shash::kSuffixCertificate);
}

void Spooler::ProcessMetainfo(IngestionSource* metainfo_source) {
  ingestion_pipeline_->Process(metainfo_source, false, shash::kSuffixMetainfo);
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

void Spooler::FinalizeSession(bool commit, const std::string &old_root_hash,
                              const std::string &new_root_hash,
                              const RepositoryTag &tag) const {
  uploader_->FinalizeSession(commit, old_root_hash, new_root_hash, tag);
}

unsigned int Spooler::GetNumberOfErrors() const {
  return uploader_->GetNumberOfErrors();
}

}  // namespace upload
