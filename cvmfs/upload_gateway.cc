/**
 * This file is part of the CernVM File System.
 */

#include "upload_gateway.h"

#include <cassert>
#include <limits>
#include <vector>

#include "gateway_util.h"
#include "util/exception.h"
#include "util/logging.h"
#include "util/string.h"

namespace upload {

GatewayStreamHandle::GatewayStreamHandle(const CallbackTN *commit_callback,
                                         ObjectPack::BucketHandle bkt)
    : UploadStreamHandle(commit_callback), bucket(bkt) { }

bool GatewayUploader::WillHandle(const SpoolerDefinition &spooler_definition) {
  return spooler_definition.driver_type == SpoolerDefinition::Gateway;
}

bool GatewayUploader::ParseSpoolerDefinition(
    const SpoolerDefinition &spooler_definition,
    GatewayUploader::Config *config) {
  const std::string &config_string = spooler_definition.spooler_configuration;
  if (!config) {
    LogCvmfs(kLogUploadGateway, kLogStderr, "\"config\" argument is NULL");
    return false;
  }

  if (spooler_definition.session_token_file.empty()) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "Failed to configure gateway uploader. "
             "Missing session token file.\n");
    return false;
  }
  config->session_token_file = spooler_definition.session_token_file;

  if (spooler_definition.key_file.empty()) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "Failed to configure gateway uploader. "
             "Missing API key file.\n");
    return false;
  }
  config->key_file = spooler_definition.key_file;

  // Repo address, e.g. http://my.repo.address
  config->api_url = config_string;

  return true;
}

GatewayUploader::GatewayUploader(const SpoolerDefinition &spooler_definition)
    : AbstractUploader(spooler_definition)
    , config_()
    , session_context_(new SessionContext()) {
  assert(spooler_definition.IsValid()
         && spooler_definition.driver_type == SpoolerDefinition::Gateway);

  if (!ParseSpoolerDefinition(spooler_definition, &config_)) {
    PANIC(kLogStderr, "Error in parsing the spooler definition");
  }

  atomic_init32(&num_errors_);
}

GatewayUploader::~GatewayUploader() {
  if (session_context_) {
    delete session_context_;
  }
}

bool GatewayUploader::Create() {
  LogCvmfs(kLogUploadGateway, kLogStderr,
           "cannot create repository storage area when using the gateway");
  return false;
}

bool GatewayUploader::Initialize() {
  if (!AbstractUploader::Initialize()) {
    return false;
  }
  std::string session_token;
  ReadSessionTokenFile(config_.session_token_file, &session_token);

  std::string key_id;
  std::string secret;
  if (!ReadKey(config_.key_file, &key_id, &secret)) {
    return false;
  }

  return session_context_->Initialize(config_.api_url, session_token, key_id,
                                      secret);
}

bool GatewayUploader::FinalizeSession(bool commit,
                                      const std::string &old_root_hash,
                                      const std::string &new_root_hash,
                                      const RepositoryTag &tag) {
  return session_context_->Finalize(commit, old_root_hash, new_root_hash, tag);
}

void GatewayUploader::WaitForUpload() const {
  session_context_->WaitForUpload();
}

std::string GatewayUploader::name() const { return "HTTP"; }

void GatewayUploader::DoRemoveAsync(const std::string & /*file_to_delete*/) {
  atomic_inc32(&num_errors_);
  Respond(NULL, UploaderResults());
}

bool GatewayUploader::Peek(const std::string & /*path*/) { return false; }

// TODO(jpriessn): implement Mkdir on gateway server-side
bool GatewayUploader::Mkdir(const std::string &path) { return true; }

bool GatewayUploader::PlaceBootstrappingShortcut(
    const shash::Any & /*object*/) {
  return false;
}

unsigned int GatewayUploader::GetNumberOfErrors() const {
  return atomic_read32(&num_errors_);
}

void GatewayUploader::DoUpload(const std::string &remote_path,
                               IngestionSource *source,
                               const CallbackTN *callback) {
  UniquePtr<GatewayStreamHandle> handle(
      new GatewayStreamHandle(callback, session_context_->NewBucket()));

  bool rvb = source->Open();
  if (!rvb) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "File upload - could not open local file.");
    BumpErrors();
    Respond(callback, UploaderResults(1, source->GetPath()));
    return;
  }

  unsigned char hash_ctx[shash::kMaxContextSize];
  shash::ContextPtr hash_ctx_ptr(spooler_definition().hash_algorithm, hash_ctx);
  shash::Init(hash_ctx_ptr);
  std::vector<char> buf(1024);
  ssize_t read_bytes = 0;
  do {
    read_bytes = source->Read(&buf[0], buf.size());
    assert(read_bytes >= 0);
    ObjectPack::AddToBucket(&buf[0], read_bytes, handle->bucket);
    shash::Update(reinterpret_cast<unsigned char *>(&buf[0]), read_bytes,
                  hash_ctx_ptr);
  } while (static_cast<size_t>(read_bytes) == buf.size());
  source->Close();
  shash::Any content_hash(spooler_definition().hash_algorithm);
  shash::Final(hash_ctx_ptr, &content_hash);

  if (!session_context_->CommitBucket(ObjectPack::kNamed, content_hash,
                                      handle->bucket, remote_path)) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "File upload - could not commit bucket");
    BumpErrors();
    Respond(handle->commit_callback, UploaderResults(2, source->GetPath()));
    return;
  }

  Respond(callback, UploaderResults(0, source->GetPath()));
}

UploadStreamHandle *GatewayUploader::InitStreamedUpload(
    const CallbackTN *callback) {
  return new GatewayStreamHandle(callback, session_context_->NewBucket());
}

void GatewayUploader::StreamedUpload(UploadStreamHandle *handle,
                                     UploadBuffer buffer,
                                     const CallbackTN *callback) {
  GatewayStreamHandle *hd = dynamic_cast<GatewayStreamHandle *>(handle);
  if (!hd) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "Streamed upload - incompatible upload handle");
    BumpErrors();
    Respond(callback, UploaderResults(UploaderResults::kBufferUpload, 2));
    return;
  }

  ObjectPack::AddToBucket(buffer.data, buffer.size, hd->bucket);

  Respond(callback, UploaderResults(UploaderResults::kBufferUpload, 0));
}

void GatewayUploader::FinalizeStreamedUpload(UploadStreamHandle *handle,
                                             const shash::Any &content_hash) {
  GatewayStreamHandle *hd = dynamic_cast<GatewayStreamHandle *>(handle);
  if (!hd) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "Finalize streamed upload - incompatible upload handle");
    BumpErrors();
    Respond(handle->commit_callback,
            UploaderResults(UploaderResults::kChunkCommit, 2));
    return;
  }

  // hd->remote_path is ignored when empty
  if (!session_context_->CommitBucket(ObjectPack::kCas, content_hash,
                                      hd->bucket, hd->remote_path)) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "Finalize streamed upload - could not commit bucket");
    BumpErrors();
    Respond(handle->commit_callback,
            UploaderResults(UploaderResults::kChunkCommit, 4));
    return;
  }
  if (!content_hash.HasSuffix()
      || content_hash.suffix == shash::kSuffixPartial) {
    CountUploadedChunks();
    CountUploadedBytes(hd->bucket->size);
  } else if (content_hash.suffix == shash::kSuffixCatalog) {
    CountUploadedCatalogs();
    CountUploadedCatalogBytes(hd->bucket->size);
  }
  Respond(handle->commit_callback,
          UploaderResults(UploaderResults::kChunkCommit, 0));
}

void GatewayUploader::ReadSessionTokenFile(const std::string &token_file_name,
                                           std::string *token) {
  assert(token);
  *token = "INVALIDTOKEN";  // overwritten if reading from file works

  FILE *token_file = std::fopen(token_file_name.c_str(), "r");
  if (!token_file) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "HTTP Uploader - Could not open session token file.");
    return;
  }

  GetLineFile(token_file, token);
  fclose(token_file);
}

bool GatewayUploader::ReadKey(const std::string &key_file, std::string *key_id,
                              std::string *secret) {
  return gateway::ReadKeys(key_file, key_id, secret);
}

int64_t GatewayUploader::DoGetObjectSize(const std::string &file_name) {
  return -EOPNOTSUPP;
}

void GatewayUploader::BumpErrors() const { atomic_inc32(&num_errors_); }

}  // namespace upload
