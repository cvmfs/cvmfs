#include "upload_riak.h"

#include "duplex_curl.h"
#include "logging.h"

using namespace upload;

RiakSpoolerBackend::RiakSpoolerBackend(const std::string &config_file_path) :
  config_file_path_(config_file_path),
  initialized_(false)
{}

RiakSpoolerBackend::~RiakSpoolerBackend()
{
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

  initialized_ = true;
  return true;
}

void RiakSpoolerBackend::Copy(const std::string &local_path,
                              const std::string &remote_path,
                              const bool move,
                              std::string &response) {
  // TODO: do something useful here...

  LogCvmfs(kLogSpooler, kLogDebug, "called COPY for local_path: %s",
    local_path.c_str());

  CreateResponseMessage(response, 100, local_path, "");
}

void RiakSpoolerBackend::Process(const std::string &local_path,
                                 const std::string &remote_dir,
                                 const std::string &file_suffix,
                                 const bool move,
                                 std::string &response) {
  // TODO: do something useful here...

  LogCvmfs(kLogSpooler, kLogDebug, "called PROCESS for local_path: %s",
    local_path.c_str());

  CreateResponseMessage(response, 103, local_path, "");
}

bool RiakSpoolerBackend::IsReady() const {
  const bool ready = AbstractSpoolerBackend::IsReady();
  return ready && initialized_;
}