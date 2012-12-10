/**
 * This file is part of the CernVM File System.
 */

#include "upload_riak.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>

#include "duplex_curl.h"
#include "logging.h"

#include "util.h"

using namespace upload;

RiakPushWorker::Context* RiakPushWorker::GenerateContext(
                            SpoolerImpl<RiakPushWorker>      *master,
                            const Spooler::SpoolerDefinition &spooler_definition) {
  assert (spooler_definition.IsValid() &&
          spooler_definition.driver_type == Spooler::SpoolerDefinition::Riak);

  std::vector<std::string> upstream_url_vector =
    SplitString(spooler_definition.spooler_description, '@');
  return new Context(master, upstream_url_vector);
}


int RiakPushWorker::GetNumberOfWorkers(const Context *context) {
  return std::min((int)context->upstream_urls.size(), GetNumberOfCpuCores()) * 5;
}


const std::string& RiakPushWorker::Context::AcquireUpstreamUrl() const {
  assert (upstream_urls.size() > 0);
  // get an unique upstream URL and set the pointer to the next one...
  const std::string& result = upstream_urls[next_upstream_url_];
  next_upstream_url_ = (next_upstream_url_ + 1) % upstream_urls.size();
  return result;
}


bool RiakPushWorker::DoGlobalInitialization() {
  if (curl_global_init(CURL_GLOBAL_ALL) != CURLE_OK) {
    LogCvmfs(kLogSpooler, kLogWarning, "Failed to initialize cURL library");
    return false;
  }

  return true;
}


void RiakPushWorker::DoGlobalCleanup() {
  curl_global_cleanup();
}


RiakPushWorker::RiakPushWorker(Context* context) :
  context_(context),
  initialized_(false),
  http_headers_download_(NULL),
  compression_time_aggregated_(0),
  upload_time_aggregated_(0),
  curl_upload_time_aggregated_(0),
  curl_get_vclock_time_aggregated_(0),
  curl_connection_time_aggregated_(0),
  curl_connections_(0),
  curl_upload_speed_aggregated_(0),
  upload_jobs_count_(0)
{
  LockGuard<Context> lock(context);
  upstream_url_ = context->AcquireUpstreamUrl();
}


RiakPushWorker::~RiakPushWorker() {
  curl_easy_cleanup(curl_upload_);
  curl_easy_cleanup(curl_download_);
  curl_slist_free_all(http_headers_download_);

  if (upload_jobs_count_ == 0) {
    LogCvmfs(kLogSpooler, kLogStdout, "Did not compress/upload anything.");
    return;
  }

  const double uploaded_jobs = (double)upload_jobs_count_;
  LogCvmfs(kLogSpooler, kLogStdout, "Statistics:\n"
                                    "Avg Compression time:          %f s\n"
                                    "Avg Uploading time:            %f s\n"
                                    "Avg CURL upload time:          %f s\n"
                                    "Avg CURL Vclock retrieve time: %f s\n"
                                    "Avg CURL connection time:      %f s\n"
                                    "CURL connections:              %d\n"
                                    "Avg Data Upload speed:         %f kB/s",

           (compression_time_aggregated_     / uploaded_jobs),
           (upload_time_aggregated_          / uploaded_jobs),
           (curl_upload_time_aggregated_     / uploaded_jobs),
           (curl_get_vclock_time_aggregated_ / uploaded_jobs),
           (curl_connection_time_aggregated_ / uploaded_jobs*2),
            curl_connections_,
           (curl_upload_speed_aggregated_    / uploaded_jobs / 1024.0));
}


bool RiakPushWorker::Initialize() {
  bool retval = AbstractPushWorker::Initialize();
  if (!retval)
    return false;

  LogCvmfs(kLogSpooler, kLogVerboseMsg, "Configuring cURL handles for putting "
                                        "files into a Riak instance");

  if (!InitUploadHandle() || !InitDownloadHandle()) {
    LogCvmfs(kLogSpooler, kLogStderr, "failed to initialize cURL handle.");
    return false;
  }

  // all done
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "successfully initialized Riak "
                                        "spooler backend");

  initialized_ = true;
  return true;
}


bool RiakPushWorker::InitUploadHandle() {
  // initialize cURL handle
  curl_upload_ = curl_easy_init();
  if (! curl_upload_) {
    LogCvmfs(kLogSpooler, kLogStderr, "failed to initialize cURL handle.");
    return false;
  }

  // configure cURL handle
  if (curl_easy_setopt(curl_upload_, CURLOPT_NOPROGRESS, 1L)    != CURLE_OK)
    return false;
  if (curl_easy_setopt(curl_upload_, CURLOPT_TCP_KEEPALIVE, 1L) != CURLE_OK)
    return false;
  if (curl_easy_setopt(curl_upload_, CURLOPT_UPLOAD, 1L)        != CURLE_OK)
    return false;

  return true;
}


bool RiakPushWorker::InitDownloadHandle() {
  // initialize cURL handle
  curl_download_ = curl_easy_init();
  if (! curl_download_) {
    LogCvmfs(kLogSpooler, kLogStderr, "failed to initialize cURL handle.");
    return false;
  }

  // configure cURL handle
  if (curl_easy_setopt(curl_download_, CURLOPT_NOPROGRESS, 1L)    != CURLE_OK)
    return false;
  if (curl_easy_setopt(curl_download_, CURLOPT_TCP_KEEPALIVE, 1L) != CURLE_OK)
    return false;
  if (curl_easy_setopt(curl_download_, CURLOPT_HTTPGET, 1L)       != CURLE_OK)
    return false;
  if (curl_easy_setopt(curl_download_, CURLOPT_NOBODY, 1L)        != CURLE_OK)
    return false;

  // configure download headers
  http_headers_download_ = curl_slist_append(http_headers_download_, "Accept: */*");
  if (curl_easy_setopt(curl_download_, CURLOPT_HTTPHEADER, http_headers_download_) != CURLE_OK)
    return false;

  // configure header readout callback
  if (curl_easy_setopt(curl_download_, CURLOPT_HEADERFUNCTION, &ReadHeaderCallback) != CURLE_OK)
    return false;

  return true;
}


void RiakPushWorker::ProcessCopyJob(StorageCopyJob *copy_job) {
  if (copy_job->move()) {
    LogCvmfs(kLogSpooler, kLogStderr, "RiakPushWorker does not support "
                                      "move at the moment.");
    copy_job->Failed();
    return;
  }

  const std::string& remote_path = copy_job->remote_path();
  const std::string& local_path  = copy_job->local_path();

  // copy is always critical... set third parameter to 'true'
  const int retval = PushFileToRiak(GenerateRiakKey(remote_path),
                                    local_path,
                                    true);

  copy_job->Finished(retval);
}


void RiakPushWorker::ProcessCompressionJob(
                                      StorageCompressionJob *compression_job) {
  if (compression_job->move()) {
    LogCvmfs(kLogSpooler, kLogStderr, "RiakPushWorker does not support "
                                      "move at the moment.");
    compression_job->Failed();
    return;
  }

  compression_stopwatch_.Reset();
  upload_stopwatch_.Reset();

  const std::string &local_path   = compression_job->local_path();
        hash::Any   &content_hash = compression_job->content_hash();

  // compress the file to a temporary location
  static const std::string tmp_dir = "/ramdisk";
  std::string tmp_file_path;
  hash::Any compressed_hash(hash::kSha1);

  compression_stopwatch_.Start();

  if (! CompressToTempFile(local_path,
                           tmp_dir,
                           &tmp_file_path,
                           &content_hash) ) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to compress file before pushing "
                                      "to Riak: %s",
             local_path.c_str());

    compression_job->Failed(101);
    return;
  }

  compression_stopwatch_.Stop();

  // push to Riak
  upload_stopwatch_.Start();
  const int retval = PushFileToRiak(GenerateRiakKey(compression_job),
                                    tmp_file_path);
  upload_stopwatch_.Stop();

  // retrieve the stopwatch values
  upload_time_aggregated_ += upload_stopwatch_.GetTime();
  compression_time_aggregated_ += compression_stopwatch_.GetTime();
  upload_jobs_count_++;

  // clean up and go home
  compression_job->Finished(retval);
  unlink(tmp_file_path.c_str());
}


bool RiakPushWorker::CompressToTempFile(const std::string &source_file_path,
                                        const std::string &destination_dir,
                                        std::string       *tmp_file_path,
                                        hash::Any         *content_hash) const {
  // Create a temporary file at the given destination directory
  FILE *fcas = CreateTempFile(destination_dir + "/cvmfs", 0777, "w", 
                              tmp_file_path);
  if (fcas == NULL) {
    LogCvmfs(kLogSpooler, kLogStderr, "failed to create temporary file %s",
             tmp_file_path->c_str());
    return false;
  }

  // Compress the provided source file and write the result into the temporary.
  // Additionally computes the content hash of the compressed data
  int retval = zlib::CompressPath2File(source_file_path, fcas, content_hash);
  if (! retval) {
    LogCvmfs(kLogSpooler, kLogStderr, "failed to compress file %s to temporary "
                                      "file %s",
             source_file_path.c_str(), tmp_file_path->c_str());

    unlink(tmp_file_path->c_str());
    return false;
  }
  fclose(fcas);

  return true;
}


/**
 * This method looks for the X-Riak-Vclock header and copies its value into the 
 * std::string provided through the void *userdata pointer.
 * Handle this thing with absolute care!
 */
size_t RiakPushWorker::ReadHeaderCallback(void *ptr, 
                                          size_t size,
                                          size_t nmemb,
                                          void *userdata) {
  // vector clock header description
  // Example:
  // X-Riak-Vclock: a85hYGBgzGDKBVIceQ1fzgWYubNkMCUy5rEybNMzOcWXBQA=\r\n
  // ~~~ name ~~~~  ~~~~~~~~~~~~~~~~~~~ clock ~~~~~~~~~~~~~~~~~~~~~~
  //              ^~ padding             carriage return / line feed ~^
  static const char*  vclock_header_name        = "X-Riak-Vclock";
  static const size_t vclock_header_name_length = strlen(vclock_header_name);
  static const size_t vclock_header_padding     = 2;
  static const size_t vclock_header_newline     = 2;

  // incoming data shaping
  const char   *bytes          = (const char*)ptr;
  const size_t  bytes_received = size*nmemb;
  std::string  &result         = *(reinterpret_cast<std::string*>(userdata));

  // check for the vector clock header
  if (strncmp(bytes,
              vclock_header_name,
              std::min(bytes_received, vclock_header_name_length)) == 0)
  {
    // compute the length and position of the vector clock data
    const char* start_of_clock_value = bytes                     +
                                       vclock_header_name_length +
                                       vclock_header_padding;
    const size_t vector_clock_length = bytes_received            -
                                       vclock_header_name_length -
                                       vclock_header_padding     -
                                       vclock_header_newline;

    // copy the vector clock data into the result string
    result = std::string(start_of_clock_value, vector_clock_length);
  }

  // cURL needs to receive the exact number of incoming bytes as return value
  return bytes_received;
}


bool RiakPushWorker::GetVectorClock(const std::string &key,
                                          std::string &vector_clock) {
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "checking if key %s already exists",
           key.c_str());

  // generate the request URL
  const std::string url = CreateRequestUrl(key);
  if (curl_easy_setopt(curl_download_, CURLOPT_URL, url.c_str()) != CURLE_OK)
    return false;

  // set the vector_clock to be written in the ReadHeaderCallback()
  if (curl_easy_setopt(curl_download_, CURLOPT_WRITEHEADER, (void*)&vector_clock) != CURLE_OK)
    return false;

  // do the actual business
  CURLcode res = curl_easy_perform(curl_download_);
  if (res != CURLE_OK) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to check existence of key %s in "
                                      "Riak because: '%s'",
             key.c_str(), curl_easy_strerror(res));
    return false;
  }

  // check if all went fine
  long response_code;
  res = curl_easy_getinfo(curl_download_, CURLINFO_RESPONSE_CODE, &response_code);
  if (res != CURLE_OK) {
    LogCvmfs(kLogSpooler, kLogStderr, "Unable to retrieve response code for "
                                      "key %s in Riak node %s",
             key.c_str(), url.c_str());
    return false;
  }

  // check if object was found
  if (response_code != 200 && response_code != 304)
    return false;

  if (!CollectVclockFetchStatistics()) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to collect vclock fetch statistics");
    return false;
  }

  // we found an object and therefore most likely vector_clock is set now
  return true;
}

int RiakPushWorker::PushFileToRiak(const std::string &key,
                                   const std::string &file_path,
                                   const bool         is_critical) {
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "pushing file %s to Riak using key %s",
           file_path.c_str(), key.c_str());

  const std::string url = CreateRequestUrl(key, is_critical);
  FILE *hd_src;

  long response_code;
  double uploaded_bytes;

  CURLcode res;
  struct curl_slist *headers = NULL;
  int      retcode = -1;

  std::string vector_clock;

  // find the size of the file to be uploaded
  struct stat file_info;
  int hd = open(file_path.c_str(), O_RDONLY);
  if (hd < 0) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to stat file %s",
             file_path.c_str());
    retcode = 1;
    close(hd);
    goto out;
  }
  fstat(hd, &file_info);
  close(hd);

  // open the file for reading
  hd_src = fopen(file_path.c_str(), "rb");
  if (hd_src == NULL) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to open file %s for reading.",
             file_path.c_str());
    retcode = 2;
    goto out;
  }

  // configure headers
  headers = curl_slist_append(headers, "Content-Type: application/octet-stream");

  // check if key already exists and find out about its current vector clock
  if (GetVectorClock(key, vector_clock)) {
    headers = curl_slist_append(headers, ("X-Riak-Vclock: " + vector_clock).c_str());
  }

  // set headers
  if (curl_easy_setopt(curl_upload_, CURLOPT_HTTPHEADER, headers) != CURLE_OK)
    return false;

  // set url for Riak put command
  if (curl_easy_setopt(curl_upload_, CURLOPT_URL, url.c_str()) != CURLE_OK) {
    retcode = 3;
    goto out;
  }

  // set file size of the file to be uploaded
  if (curl_easy_setopt(curl_upload_, CURLOPT_INFILESIZE_LARGE,
                       (curl_off_t)file_info.st_size) != CURLE_OK) {
    retcode = 4;
    goto out;
  }

  // specify the file handle to be uploaded
  if (curl_easy_setopt(curl_upload_, CURLOPT_READDATA, hd_src) != CURLE_OK) {
    retcode = 5;
    goto out;
  }

  // do the actual business
  res = curl_easy_perform(curl_upload_);
  if (res != CURLE_OK) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to upload %s to Riak node %s "
                                      "because: '%s'",
             file_path.c_str(), url.c_str(), curl_easy_strerror(res));
    retcode = 6;
    goto out;
  }

  // check if all went fine
  res = curl_easy_getinfo(curl_upload_, CURLINFO_RESPONSE_CODE, &response_code);
  if (res != CURLE_OK) {
    LogCvmfs(kLogSpooler, kLogStderr, "Unable to retrieve response code");
    retcode = 7;
    goto out;
  }

  res = curl_easy_getinfo(curl_upload_, CURLINFO_SIZE_UPLOAD, &uploaded_bytes);
  if (res != CURLE_OK) {
    LogCvmfs(kLogSpooler, kLogStderr, "Unable to retrieve number of uploaded "
                                      "bytes");
    retcode = 8;
    goto out;
  }

  if (response_code != 204 && response_code != 200) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to upload %s to Riak node %s "
                                      "response code from Riak was: %d",
             file_path.c_str(), url.c_str(), response_code);
    retcode = 9;
    goto out;
  }

  if ((int)uploaded_bytes != file_info.st_size) {
    LogCvmfs(kLogSpooler, kLogStderr, "Did not upload the correct amount of "
                                      "data to Riak: %d/%d bytes uploaded for "
                                      "file %s to Riak node: %s",
             (int)uploaded_bytes,
             file_info.st_size,
             file_path.c_str(),
             url.c_str());
    retcode = 10;
    goto out;
  }

  if (!CollectUploadStatistics()) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to grab statistics data.");
    retcode = 11;
    goto out;
  }

  // all went well...
  retcode = 0;

out:
  // close the uploaded file
  fclose(hd_src);
  curl_slist_free_all(headers);
  return retcode;
}


bool RiakPushWorker::CollectUploadStatistics() {
  CURLcode res;

  double upload_time;
  double connection_time;
  long   connections;
  double upload_speed;

  res = curl_easy_getinfo(curl_upload_, CURLINFO_TOTAL_TIME, &upload_time);
  if (res != CURLE_OK) return false;
  curl_upload_time_aggregated_ += upload_time;

  res = curl_easy_getinfo(curl_upload_, CURLINFO_CONNECT_TIME, &connection_time);
  if (res != CURLE_OK) return false;
  curl_connection_time_aggregated_ += connection_time;

  res = curl_easy_getinfo(curl_upload_, CURLINFO_NUM_CONNECTS, &connections);
  if (res != CURLE_OK) return false;
  curl_connections_ += connections;

  res = curl_easy_getinfo(curl_upload_, CURLINFO_SPEED_UPLOAD, &upload_speed);
  if (res != CURLE_OK) return false;
  curl_upload_speed_aggregated_ += upload_speed;

  return true;
}


bool RiakPushWorker::CollectVclockFetchStatistics() {
  CURLcode res;
  double request_time;
  double connection_time;

  res = curl_easy_getinfo(curl_download_, CURLINFO_TOTAL_TIME, &request_time);
  if (res != CURLE_OK) return false;
  curl_get_vclock_time_aggregated_ += request_time;

  res = curl_easy_getinfo(curl_download_, CURLINFO_CONNECT_TIME, &connection_time);
  if (res != CURLE_OK) return false;
  curl_connection_time_aggregated_ += connection_time;

  return true;
}


std::string RiakPushWorker::GenerateRiakKey(
                          const StorageCompressionJob *compression_job) const {
  assert (!compression_job->content_hash().IsNull());

  return compression_job->remote_dir()              + 
         compression_job->content_hash().ToString() + 
         compression_job->file_suffix();
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


std::string RiakPushWorker::CreateRequestUrl(const std::string &key,
                                             const bool is_critical) const {
  return upstream_url_ + "/" + key + (is_critical ? "?w=all" : "");
}


bool RiakPushWorker::IsReady() const {
  const bool ready = AbstractPushWorker::IsReady();
  return ready && initialized_;
}
