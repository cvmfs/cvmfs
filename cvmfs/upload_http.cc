/**
 * This file is part of the CernVM File System.
 */

#include "upload_http.h"

#include "util/string.h"

namespace {
void HTTPLog(const char* format_string, ...) {
  va_list argp;
  LogCvmfs(kLogUploadHttp, kLogStderr, format_string, argp);
}

void LogBadConfig(const std::string& config) {
  HTTPLog(
      "Failed to parse spooler configuration string '%s'.\n"
      "Provide: http://<repository_address>:<port>/<api_path>",
      config.c_str());
}
}

namespace upload {

HttpUploader::HttpUploader(const SpoolerDefinition& spooler_definition)
    : AbstractUploader(spooler_definition), config_() {
  assert(spooler_definition.IsValid() &&
         spooler_definition.driver_type == SpoolerDefinition::HTTP);

  if (!ParseSpoolerDefinition(spooler_definition, &config_)) {
    abort();
  }

  HTTPLog(
      "HTTP uploader configuration:\n"
      "  Repository address: %s\n"
      "  Port: %d\n"
      "  API path: %s\n",
      config_.repository_address.c_str(), config_.port,
      config_.api_path.c_str());
}

bool HttpUploader::WillHandle(const SpoolerDefinition& spooler_definition) {
  return spooler_definition.driver_type == SpoolerDefinition::HTTP;
}

HttpUploader::~HttpUploader() {}

UploadStreamHandle* HttpUploader::InitStreamedUpload(
    const CallbackTN* /*callback*/) {
  return reinterpret_cast<UploadStreamHandle*>(NULL);
}

bool HttpUploader::Remove(const std::string& /*file_to_delete*/) {
  return false;
}

bool HttpUploader::Peek(const std::string& /*path*/) const { return false; }

bool HttpUploader::PlaceBootstrappingShortcut(
    const shash::Any& /*object*/) const {
  return false;
}

unsigned int HttpUploader::GetNumberOfErrors() const { return 0; }

bool HttpUploader::ParseSpoolerDefinition(
    const SpoolerDefinition& spooler_definition, HttpUploader::Config* config) {
  const std::string& config_string = spooler_definition.spooler_configuration;
  if (!config) {
    HTTPLog("\"config\" argument is NULL");
    return false;
  }

  if (!HasPrefix(config_string, "http", false)) {
    LogBadConfig(config_string);
    return false;
  }

  const std::vector<std::string> tokens1 = SplitString(config_string, ':');
  if (tokens1.size() != 3) {
    LogBadConfig(config_string);
    return false;
  }

  // Repo address, e.g. http://my.repo.address
  config->repository_address = tokens1[0] + ":" + tokens1[1];

  std::vector<std::string> tokens2 = SplitString(tokens1[2], '/');
  if (tokens2.size() < 1) {
    LogBadConfig(config_string);
    return false;
  }

  uint64_t port;
  if (!String2Uint64Parse(tokens2[0], &port)) {
    LogBadConfig(config_string);
  }
  if (port > std::numeric_limits<uint16_t>::max()) {
    HTTPLog("Invalid port number in spooler configuration");
    return false;
  }
  config->port = port;

  tokens2.erase(tokens2.begin());
  config->api_path = JoinStrings(tokens2, "/");

  return true;
}

void HttpUploader::FileUpload(const std::string& /*local_path*/,
                              const std::string& /*remote_path*/,
                              const CallbackTN* /*callback*/) {}

void HttpUploader::StreamedUpload(UploadStreamHandle* /*handle*/,
                                  CharBuffer* /*buffer*/,
                                  const CallbackTN* /*callback*/) {}

void HttpUploader::FinalizeStreamedUpload(UploadStreamHandle* /*handle*/,
                                          const shash::Any& /*content_hash*/) {}

}  // namespace upload
