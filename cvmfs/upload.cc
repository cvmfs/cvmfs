/**
 * This file is part of the CernVM File System.
 */

#include "upload.h"

#include <vector>

#include "util/concurrency.h"
#include "util/shared_ptr.h"

namespace upload {

Spooler *Spooler::Construct(const SpoolerDefinition &spooler_definition,
                            perf::StatisticsTemplate *statistics) {
  Spooler *result = new Spooler(spooler_definition);
  if (!result->Initialize(statistics)) {
    delete result;
    result = NULL;
  }
  return result;
}

Spooler::Spooler(const SpoolerDefinition &spooler_definition)
    : spooler_definition_(spooler_definition) { }

Spooler::~Spooler() {
  FinalizeSession(false);
  if (uploader_.IsValid()) {
    uploader_->TearDown();
  }
}

std::string Spooler::backend_name() const { return uploader_->name(); }

bool Spooler::Initialize(perf::StatisticsTemplate *statistics) {
  // configure the uploader environment
  uploader_ = AbstractUploader::Construct(spooler_definition_);
  if (!uploader_.IsValid()) {
    LogCvmfs(kLogSpooler, kLogWarning,
             "Failed to initialize backend upload "
             "facility in Spooler.");
    return false;
  }

  if (statistics != NULL) {
    uploader_->InitCounters(statistics);
  }

  // configure the file processor context
  ingestion_pipeline_ = new IngestionPipeline(uploader_.weak_ref(),
                                              spooler_definition_);
  ingestion_pipeline_->RegisterListener(&Spooler::ProcessingCallback, this);
  ingestion_pipeline_->Spawn();

  // all done...
  return true;
}

bool Spooler::Create() { return uploader_->Create(); }

void Spooler::Process(IngestionSource *source, const bool allow_chunking) {
  ingestion_pipeline_->Process(source, allow_chunking);
}

void Spooler::ProcessCatalog(const std::string &local_path) {
  ingestion_pipeline_->Process(new FileIngestionSource(local_path), false,
                               shash::kSuffixCatalog);
}

void Spooler::ProcessHistory(const std::string &local_path) {
  ingestion_pipeline_->Process(new FileIngestionSource(local_path), false,
                               shash::kSuffixHistory);
}

void Spooler::ProcessCertificate(const std::string &local_path) {
  ingestion_pipeline_->Process(new FileIngestionSource(local_path), false,
                               shash::kSuffixCertificate);
}

void Spooler::ProcessCertificate(IngestionSource *source) {
  ingestion_pipeline_->Process(source, false, shash::kSuffixCertificate);
}

void Spooler::ProcessMetainfo(const std::string &local_path) {
  ingestion_pipeline_->Process(new FileIngestionSource(local_path), false,
                               shash::kSuffixMetainfo);
}

void Spooler::ProcessMetainfo(IngestionSource *source) {
  ingestion_pipeline_->Process(source, false, shash::kSuffixMetainfo);
}

void Spooler::Upload(const std::string &local_path,
                     const std::string &remote_path) {
  uploader_->UploadFile(
      local_path, remote_path,
      AbstractUploader::MakeCallback(&Spooler::UploadingCallback, this));
}

void Spooler::Upload(const std::string &remote_path, IngestionSource *source) {
  uploader_->UploadIngestionSource(
      remote_path, source,
      AbstractUploader::MakeCallback(&Spooler::UploadingCallback, this));
  delete source;
}


void Spooler::UploadManifest(const std::string &local_path) {
  Upload(local_path, ".cvmfspublished");
}

void Spooler::UploadReflog(const std::string &local_path) {
  Upload(local_path, ".cvmfsreflog");
}

void Spooler::RemoveAsync(const std::string &file_to_delete) {
  uploader_->RemoveAsync(file_to_delete);
}

bool Spooler::Peek(const std::string &path) const {
  return uploader_->Peek(path);
}

bool Spooler::Mkdir(const std::string &path) { return uploader_->Mkdir(path); }

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
  return uploader_->FinalizeSession(commit, old_root_hash, new_root_hash, tag);
}

unsigned int Spooler::GetNumberOfErrors() const {
  return uploader_->GetNumberOfErrors();
}

}  // namespace upload
