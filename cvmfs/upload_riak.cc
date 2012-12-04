#include "upload_riak.h"

#include <algorithm>

#include "duplex_curl.h"
#include "logging.h"

using namespace upload;

RiakSpoolerBackend::RiakSpoolerBackend(const std::string &config_file_path) :
  config_(config_file_path),
  initialized_(false)
{}


RiakSpoolerBackend::~RiakSpoolerBackend() {
  curl_global_cleanup();
}


bool RiakSpoolerBackend::Initialize() {
  bool retval = AbstractSpoolerBackend::Initialize();
  if (!retval)
    return false;

  // read configuration
  // TODO...

  // initialize libcurl
  int cretval = curl_global_init(CURL_GLOBAL_ALL);
  if (cretval != 0) {
    LogCvmfs(kLogSpooler, kLogStderr, 
      "failed to initialize curl in riak spooler. Errorcode: %d", cretval);
    return false;
  }

  LogCvmfs(kLogSpooler, kLogVerboseMsg, "successfully initialized Riak "
                                        "spooler backend");

  initialized_ = true;
  return true;
}


void RiakSpoolerBackend::Copy(const std::string &local_path,
                              const std::string &remote_path,
                              const bool move) {
  if (move) {
    LogCvmfs(kLogSpooler, kLogStderr, "RiakSpoolerBackend does not support "
                                      "move at the moment.");
    SendResult(100, local_path);
    return;
  }

  PushFileToRiakAsync(GenerateRiakKey(remote_path),
                      local_path,
                      PushFinishedCallback(this,
                                           local_path));
}


void RiakSpoolerBackend::Process(const std::string &local_path,
                                 const std::string &remote_dir,
                                 const std::string &file_suffix,
                                 const bool move) {
  if (move) {
    LogCvmfs(kLogSpooler, kLogStderr, "RiakSpoolerBackend does not support "
                                      "move at the moment.");
    SendResult(100, local_path);
    return;
  }

  // compress the file to a temporary location
  static const std::string tmp_dir = "/tmp";
  std::string tmp_file_path;
  hash::Any compressed_hash(hash::kSha1);
  if (! CompressToTempFile(local_path,
                           tmp_dir,
                           &tmp_file_path,
                           &compressed_hash) ) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to compress file before pushing "
                                      "to Riak: %s",
             local_path.c_str());
    SendResult(101, local_path);
    return;
  }

  // push to Riak
  PushFileToRiakAsync(GenerateRiakKey(compressed_hash,
                                      remote_dir,
                                      file_suffix),
                      tmp_file_path,
                      PushFinishedCallback(this,
                                           local_path,
                                           compressed_hash)
  );
}


std::string RiakSpoolerBackend::GenerateRiakKey(const hash::Any   &compressed_hash,
                                                const std::string &remote_dir,
                                                const std::string &file_suffix) const {
  return remote_dir + compressed_hash.ToString() + file_suffix;
}


std::string RiakSpoolerBackend::GenerateRiakKey(const std::string &remote_path) const {
  std::string result = remote_path;
  std::remove(result.begin(), result.end(), '/');
  return result;
}


void RiakSpoolerBackend::PushFileToRiakAsync(const std::string          &key,
                                             const std::string          &file_path,
                                             const PushFinishedCallback &callback) {
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "pushing file %s to Riak using key %s",
           file_path.c_str(), key.c_str());

  // TOOD: actually push here...

  callback(0);
}


bool RiakSpoolerBackend::IsReady() const {
  const bool ready = AbstractSpoolerBackend::IsReady();
  return ready && initialized_;
}


// -----------------------------------------------------------------------------


RiakSpoolerBackend::RiakConfiguration::RiakConfiguration(
                                          const std::string& config_file_path) {
  // TODO: actually read the configuration from a file here
  bucket = "riak";
}