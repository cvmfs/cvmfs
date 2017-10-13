/**
 * This file is part of the CernVM File System.
 */

#include "upload_gateway.h"

#include <limits>
#include <vector>

#include "file_processing/char_buffer.h"
#include "gateway_util.h"
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
    LogCvmfs(kLogUploadGateway, kLogStderr, "\"config\" argument is NULL");
    return false;
  }

  if (spooler_definition.session_token_file.empty()) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "Failed to configure HTTP uploader. "
             "Missing session token file.\n");
    return false;
  }
  config->session_token_file = spooler_definition.session_token_file;

  if (spooler_definition.key_file.empty()) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "Failed to configure HTTP uploader. "
             "Missing HTTP API key file.\n");
    return false;
  }
  config->key_file = spooler_definition.key_file;

  // Repo address, e.g. http://my.repo.address
  config->api_url = config_string;

  return true;
}

GatewayUploader::GatewayUploader(const SpoolerDefinition& spooler_definition)
    : AbstractUploader(spooler_definition),
      config_(),
      session_context_(new SessionContext()) {
  assert(spooler_definition.IsValid() &&
         spooler_definition.driver_type == SpoolerDefinition::Gateway);

  if (!ParseSpoolerDefinition(spooler_definition, &config_)) {
    abort();
  }

  atomic_init32(&num_errors_);
}

GatewayUploader::~GatewayUploader() {
  if (session_context_) {
    delete session_context_;
  }
}

bool GatewayUploader::Initialize() {
  if (!AbstractUploader::Initialize()) {
    return false;
  }
  std::string session_token;
  if (!ReadSessionTokenFile(config_.session_token_file, &session_token)) {
    return false;
  }

  std::string key_id;
  std::string secret;
  if (!ReadKey(config_.key_file, &key_id, &secret)) {
    return false;
  }

  return session_context_->Initialize(config_.api_url, session_token, key_id,
                                      secret);
}

bool GatewayUploader::FinalizeSession(bool commit,
                                      const std::string& old_root_hash,
                                      const std::string& new_root_hash) {
  return session_context_->Finalize(commit, old_root_hash, new_root_hash);
}

void GatewayUploader::WaitForUpload() const {
  session_context_->WaitForUpload();
}

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

void GatewayUploader::FileUpload(const std::string& local_path,
                                 const std::string& remote_path,
                                 const CallbackTN* callback) {
  UniquePtr<GatewayStreamHandle> handle(
      new GatewayStreamHandle(callback, session_context_->NewBucket()));

  FILE* local_file = fopen(local_path.c_str(), "rb");
  if (!local_file) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "File upload - could not open local file.");
    BumpErrors();
    Respond(callback, UploaderResults(1, local_path));
    return;
  }

  std::vector<char> buf(1024);
  size_t read_bytes = 0;
  do {
    read_bytes = fread(&buf[0], buf.size(), 1, local_file);
    ObjectPack::AddToBucket(&buf[0], buf.size(), handle->bucket);
  } while (read_bytes == buf.size());
  fclose(local_file);

  shash::Any content_hash(spooler_definition().hash_algorithm);
  shash::HashFile(local_path, &content_hash);
  if (!session_context_->CommitBucket(ObjectPack::kNamed, content_hash,
                                      handle->bucket, remote_path)) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "File upload - could not commit bucket");
    BumpErrors();
    Respond(handle->commit_callback, UploaderResults(2, local_path));
    return;
  }

  Respond(callback, UploaderResults(0, local_path));
}

UploadStreamHandle* GatewayUploader::InitStreamedUpload(
    const CallbackTN* callback) {
  return new GatewayStreamHandle(callback, session_context_->NewBucket());
}

void GatewayUploader::StreamedUpload(UploadStreamHandle* handle,
                                     CharBuffer* buffer,
                                     const CallbackTN* callback) {
  if (!buffer->IsInitialized()) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "Streamed upload - input buffer is not initialized");
    BumpErrors();
    Respond(callback, UploaderResults(UploaderResults::kBufferUpload, 1));
    return;
  }

  GatewayStreamHandle* hd = dynamic_cast<GatewayStreamHandle*>(handle);
  if (!hd) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "Streamed upload - incompatible upload handle");
    BumpErrors();
    Respond(callback, UploaderResults(UploaderResults::kBufferUpload, 2));
    return;
  }

  ObjectPack::AddToBucket(buffer->ptr(), buffer->used_bytes(), hd->bucket);

  Respond(callback, UploaderResults(UploaderResults::kBufferUpload, 0));
}

void GatewayUploader::FinalizeStreamedUpload(UploadStreamHandle* handle,
                                             const shash::Any& content_hash) {
  GatewayStreamHandle* hd = dynamic_cast<GatewayStreamHandle*>(handle);
  if (!hd) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "Finalize streamed upload - incompatible upload handle");
    BumpErrors();
    Respond(handle->commit_callback,
            UploaderResults(UploaderResults::kChunkCommit, 2));
    return;
  }

  if (!session_context_->CommitBucket(ObjectPack::kCas, content_hash,
                                      hd->bucket, "")) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "Finalize streamed upload - could not commit bucket");
    BumpErrors();
    Respond(handle->commit_callback,
            UploaderResults(UploaderResults::kChunkCommit, 4));
    return;
  }

  Respond(handle->commit_callback,
          UploaderResults(UploaderResults::kChunkCommit, 0));
}

bool GatewayUploader::ReadSessionTokenFile(const std::string& token_file_name,
                                           std::string* token) {
  if (!token) {
    return false;
  }

  FILE* token_file = std::fopen(token_file_name.c_str(), "r");
  if (!token_file) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "HTTP Uploader - Could not open session token "
             "file. Aborting.");
    return false;
  }

  bool ret = GetLineFile(token_file, token);
  fclose(token_file);

  return ret;
}

bool GatewayUploader::ReadKey(const std::string& key_file, std::string* key_id,
                              std::string* secret) {
  return gateway::ReadKeys(key_file, key_id, secret);
}

void GatewayUploader::BumpErrors() const { atomic_inc32(&num_errors_); }

}  // namespace upload
