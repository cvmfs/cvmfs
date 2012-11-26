#include "upload_riak.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>

#include "duplex_curl.h"
#include "logging.h"

using namespace upload;

RiakPushWorker::RiakPushWorker(const std::string &upstream_urls) :
  config_(upstream_urls),
  initialized_(false)
{}


RiakPushWorker::~RiakPushWorker() {
  curl_easy_cleanup(curl_);
  curl_slist_free_all(http_headers_);
  curl_global_cleanup();
}


bool RiakPushWorker::Initialize() {
  bool retval = AbstractPushWorker::Initialize();
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

  // initialize cURL handle
  curl_ = curl_easy_init();
  if (! curl_) {
    LogCvmfs(kLogSpooler, kLogStderr, "failed to initialize cURL handle.");
    return false;
  }

  // configure cURL handle
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "Configuring cURL handle for putting "
                                        "files into a Riak instance");
  if (curl_easy_setopt(curl_, CURLOPT_NOPROGRESS, 1L)    != CURLE_OK) return false;
  if (curl_easy_setopt(curl_, CURLOPT_TCP_KEEPALIVE, 1L) != CURLE_OK) return false;
  if (curl_easy_setopt(curl_, CURLOPT_UPLOAD, 1L)        != CURLE_OK) return false;

  http_headers_ = curl_slist_append(http_headers_, "Content-Type: application/octet-stream");
  if (curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, http_headers_) != CURLE_OK) return false;

  // all done
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "successfully initialized Riak "
                                        "spooler backend");

  initialized_ = true;
  return true;
}


bool RiakPushWorker::ProcessJob(StoragePushJob *job) {

  return false;
}


void RiakPushWorker::Copy(const std::string &local_path,
                              const std::string &remote_path,
                              const bool move) {
  if (move) {
    LogCvmfs(kLogSpooler, kLogStderr, "RiakPushWorker does not support "
                                      "move at the moment.");
    //SendResult(100, local_path);
    return;
  }

  PushFileToRiakAsync(GenerateRiakKey(remote_path),
                      local_path,
                      PushFinishedCallback(this,
                                           local_path));
}


void RiakPushWorker::Process(const std::string &local_path,
                                 const std::string &remote_dir,
                                 const std::string &file_suffix,
                                 const bool move) {
  if (move) {
    LogCvmfs(kLogSpooler, kLogStderr, "RiakPushWorker does not support "
                                      "move at the moment.");
    //SendResult(100, local_path);
    return;
  }

  // compress the file to a temporary location
  static const std::string tmp_dir = "/tmp";
  std::string tmp_file_path;
  hash::Any compressed_hash(hash::kSha1);
  // if (! CompressToTempFile(local_path,
  //                          tmp_dir,
  //                          &tmp_file_path,
  //                          &compressed_hash) ) {
  //   LogCvmfs(kLogSpooler, kLogStderr, "Failed to compress file before pushing "
  //                                     "to Riak: %s",
  //            local_path.c_str());
  //   //SendResult(101, local_path);
  //   return;
  // }

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


std::string RiakPushWorker::GenerateRiakKey(const hash::Any   &compressed_hash,
                                                const std::string &remote_dir,
                                                const std::string &file_suffix) const {
  return remote_dir + compressed_hash.ToString() + file_suffix;
}


std::string RiakPushWorker::GenerateRiakKey(const std::string &remote_path) const {
  // removes slashes (/) from the remote_path
  std::string result;
  std::remove_copy(remote_path.begin(), 
                   remote_path.end(), 
                   std::back_inserter(result), 
                   '/');
  return result;
}


void RiakPushWorker::PushFileToRiakAsync(const std::string          &key,
                                             const std::string          &file_path,
                                             const PushFinishedCallback &callback) {
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "pushing file %s to Riak using key %s",
           file_path.c_str(), key.c_str());

  CURLcode res;

  // find the size of the file to uploaded
  struct stat file_info;
  int hd = open(file_path.c_str(), O_RDONLY);
  if (hd < 0) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to stat file %s",
             file_path.c_str());
    callback(1);
    return;
  }
  fstat(hd, &file_info);
  close(hd);

  // open the file for reading
  FILE *hd_src = fopen(file_path.c_str(), "rb");
  if (hd_src == NULL) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to open file %s for reading.",
             file_path.c_str());
    callback(2);
    return;
  }

  // set url for Riak put command
  const std::string url = config_.CreateRequestUrl(key);
  if (curl_easy_setopt(curl_, CURLOPT_URL, url.c_str()) != CURLE_OK) {
    callback(3);
    goto out;
  }

  // set file size of the file to be uploaded
  if (curl_easy_setopt(curl_, CURLOPT_INFILESIZE_LARGE,
                       (curl_off_t)file_info.st_size) != CURLE_OK) {
    callback(4);
    goto out;
  }

  // specify the file handle to be uploaded
  if (curl_easy_setopt(curl_, CURLOPT_READDATA, hd_src) != CURLE_OK) {
    callback(5);
    goto out;
  }

  // do the actual business
  res = curl_easy_perform(curl_);
  if(res != CURLE_OK) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to upload file %s to Riak "
                                      "because: '%s'",
             file_path.c_str(), curl_easy_strerror(res));
    callback(6);
    goto out;
  }

  // all went well... tell the spooler frontend
  callback(0);

out:
  // close the uploaded file
  fclose(hd_src);
}


bool RiakPushWorker::IsReady() const {
  const bool ready = AbstractPushWorker::IsReady();
  return ready && initialized_;
}


// -----------------------------------------------------------------------------


RiakPushWorker::RiakConfiguration::RiakConfiguration(
                                          const std::string& upstream_urls) :
  url(upstream_urls) {}


std::string RiakPushWorker::RiakConfiguration::CreateRequestUrl(
                                          const std::string &key) const {
  return url + "/" + key;
}
