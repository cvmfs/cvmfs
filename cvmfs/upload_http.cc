/**
 * This file is part of the CernVM File System.
 */

#include "upload_http.h"

#include <limits>
#include <vector>

#include "util/string.h"

namespace {

void LogBadConfig(const std::string& config) {
  LogCvmfs(kLogUploadHttp, kLogStderr,
           "Failed to parse spooler configuration string '%s'.\n"
           "Provide: http://<repository_services_url>:<port>[/<api_root>]",
           config.c_str());
}
}

namespace upload {

HttpStreamHandle::HttpStreamHandle(const CallbackTN* commit_callback)
    : UploadStreamHandle(commit_callback) {}

bool HttpUploader::WillHandle(const SpoolerDefinition& spooler_definition) {
  return spooler_definition.driver_type == SpoolerDefinition::HTTP;
}

bool HttpUploader::ParseSpoolerDefinition(
    const SpoolerDefinition& spooler_definition, HttpUploader::Config* config) {
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

  if (!HasPrefix(config_string, "http", false) || config_string.length() <= 7) {
    LogBadConfig(config_string);
    return false;
  }

  // Repo address, e.g. http://my.repo.address
  config->api_url = config_string;

  return true;
}

HttpUploader::HttpUploader(const SpoolerDefinition& spooler_definition)
    : AbstractUploader(spooler_definition), config_(), session_context_() {
  assert(spooler_definition.IsValid() &&
         spooler_definition.driver_type == SpoolerDefinition::HTTP);

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

HttpUploader::~HttpUploader() {}

bool HttpUploader::Initialize() {
  bool ret = AbstractUploader::Initialize();
  session_context_ = new SessionContext();

  return ret && session_context_.IsValid();
}

bool HttpUploader::FinalizeSession() {
  return session_context_->FinalizeSession();
}

std::string HttpUploader::name() const { return "HTTP"; }

bool HttpUploader::Remove(const std::string& /*file_to_delete*/) {
  return false;
}

bool HttpUploader::Peek(const std::string& /*path*/) const { return false; }

bool HttpUploader::PlaceBootstrappingShortcut(
    const shash::Any& /*object*/) const {
  return false;
}

unsigned int HttpUploader::GetNumberOfErrors() const {
  return atomic_read32(&num_errors_);
}

void HttpUploader::FileUpload(const std::string& /*local_path*/,
                              const std::string& /*remote_path*/,
                              const CallbackTN* /*callback*/) {}

UploadStreamHandle* HttpUploader::InitStreamedUpload(
    const CallbackTN* /*callback*/) {
  return reinterpret_cast<UploadStreamHandle*>(NULL);
}

void HttpUploader::StreamedUpload(UploadStreamHandle* /*handle*/,
                                  CharBuffer* /*buffer*/,
                                  const CallbackTN* /*callback*/) {}

void HttpUploader::FinalizeStreamedUpload(UploadStreamHandle* /*handle*/,
                                          const shash::Any& /*content_hash*/) {}

void HttpUploader::BumpErrors() const { atomic_inc32(&num_errors_); }

}  // namespace upload
