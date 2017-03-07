/**
 * This file is part of the CernVM File System.
 */

#include "upload_gateway.h"

#include <limits>
#include <vector>

#include "file_processing/char_buffer.h"
#include "util/string.h"

namespace upload {

GatewayStreamHandle::GatewayStreamHandle(const CallbackTN* commit_callback,
                                         ObjectPack::BucketHandle bkt)
    : UploadStreamHandle(commit_callback), bucket(bkt) {}

bool GatewayUploader::WillHandle(const SpoolerDefinition& spooler_definition) {
  return spooler_definition.driver_type == SpoolerDefinition::Gateway;
}

bool GatewayUploader::ParseSpoolerDefinition(
    const SpoolerDefinition& spooler_definition,
    GatewayUploader::Config* config) {
  const std::string& config_string = spooler_definition.spooler_configuration;
  if (!config) {
    LogCvmfs(kLogUploadHttp, kLogStderr, "\"config\" argument is NULL");
    return false;
  }

  if (spooler_definition.session_token_file.empty()) {
    LogCvmfs(kLogUploadHttp, kLogStderr,
             "Failed to configure HTTP uploader. "
             "Missing session token file.\n");
    return false;
  }
  config->session_token_file = spooler_definition.session_token_file;

  // Repo address, e.g. http://my.repo.address
  config->api_url = config_string;

  return true;
}

GatewayUploader::GatewayUploader(const SpoolerDefinition& spooler_definition)
    : AbstractUploader(spooler_definition), config_(), session_context_() {
  assert(spooler_definition.IsValid() &&
         spooler_definition.driver_type == SpoolerDefinition::Gateway);

  if (!ParseSpoolerDefinition(spooler_definition, &config_)) {
    abort();
  }

  atomic_init32(&num_errors_);

  LogCvmfs(kLogUploadHttp, kLogStderr,
           "HTTP uploader configuration:\n"
           "  API URL: %s\n"
           "  Session token file: %s\n",
           config_.api_url.c_str(), config_.session_token_file.c_str());
}

GatewayUploader::~GatewayUploader() {}

bool GatewayUploader::Initialize() {
  if (!AbstractUploader::Initialize()) {
    return false;
  }
  std::string session_token;
  if (!ReadSessionTokenFile(config_.session_token_file, &session_token)) {
    return false;
  }
  return session_context_.Initialize(config_.api_url, session_token);
}

bool GatewayUploader::FinalizeSession() { return session_context_.Finalize(); }

std::string GatewayUploader::name() const { return "HTTP"; }

bool GatewayUploader::Remove(const std::string& /*file_to_delete*/) {
  return false;
}

bool GatewayUploader::Peek(const std::string& /*path*/) const { return false; }

bool GatewayUploader::PlaceBootstrappingShortcut(
    const shash::Any& /*object*/) const {
  return false;
}

unsigned int GatewayUploader::GetNumberOfErrors() const {
  return atomic_read32(&num_errors_);
}

void GatewayUploader::FileUpload(const std::string& /*local_path*/,
                                 const std::string& /*remote_path*/,
                                 const CallbackTN* /*callback*/) {}

UploadStreamHandle* GatewayUploader::InitStreamedUpload(
    const CallbackTN* callback) {
  return new GatewayStreamHandle(callback, session_context_.NewBucket());
}

void GatewayUploader::StreamedUpload(UploadStreamHandle* handle,
                                     CharBuffer* buffer,
                                     const CallbackTN* callback) {
  if (!buffer->IsInitialized()) {
    LogCvmfs(kLogUploadHttp, kLogStderr,
             "Streamed upload - input buffer is not initialized");
    Respond(callback, UploaderResults(1, buffer));
    return;
  }

  GatewayStreamHandle* hd = dynamic_cast<GatewayStreamHandle*>(handle);
  if (!hd) {
    LogCvmfs(kLogUploadHttp, kLogStderr,
             "Streamed upload - incompatible upload handle");
    Respond(callback, UploaderResults(2, buffer));
    return;
  }

  ObjectPack::AddToBucket(buffer->ptr(), buffer->used_bytes(), hd->bucket);

  Respond(callback, UploaderResults(0, buffer));
}

void GatewayUploader::FinalizeStreamedUpload(UploadStreamHandle* handle,
                                             const shash::Any& content_hash) {
  GatewayStreamHandle* hd = dynamic_cast<GatewayStreamHandle*>(handle);
  if (!hd) {
    LogCvmfs(kLogUploadHttp, kLogStderr,
             "Finalize streamed upload - incompatible upload handle");
    Respond(handle->commit_callback, UploaderResults(2));
    return;
  }

  if (!session_context_.CommitBucket(ObjectPack::kCas, content_hash, hd->bucket,
                                     "")) {
    LogCvmfs(kLogUploadHttp, kLogStderr,
             "Finalize streamed upload - could not commit bucket");
    Respond(handle->commit_callback, UploaderResults(4));
    return;
  }

  Respond(handle->commit_callback, UploaderResults(0));
}

bool GatewayUploader::ReadSessionTokenFile(const std::string& token_file_name,
                                           std::string* token) {
  if (!token) {
    return false;
  }

  FILE* token_file = std::fopen(token_file_name.c_str(), "r");
  if (!token_file) {
    LogCvmfs(kLogUploadHttp, kLogStderr,
             "HTTP Uploader - Could not open session token "
             "file. Aborting.");
    return false;
  }

  return GetLineFile(token_file, token);
}

void GatewayUploader::BumpErrors() const { atomic_inc32(&num_errors_); }

}  // namespace upload
