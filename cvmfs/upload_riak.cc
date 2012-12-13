/**
 * This file is part of the CernVM File System.
 */

#include "upload_riak.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>

#include <algorithm>

#include "duplex_curl.h"
#include "logging.h"

#include "util.h"
#include "vjson/json.h"

using namespace upload;

RiakPushWorker::Context* RiakPushWorker::GenerateContext(
                            SpoolerImpl<RiakPushWorker>      *master,
                            const Spooler::SpoolerDefinition &spooler_definition) {
  assert (spooler_definition.IsValid() &&
          spooler_definition.driver_type == Spooler::SpoolerDefinition::Riak);

  std::vector<std::string> upstream_url_vector =
    SplitString(spooler_definition.spooler_description, '@');

  const size_t max_compression_buffer_size = 1024*1024; // TOOD: make this config-
                                                        //       urable

  return new Context(master, upstream_url_vector, max_compression_buffer_size);
}


int RiakPushWorker::GetNumberOfWorkers(const Context *context) {
  return std::max((int)context->upstream_urls.size(), GetNumberOfCpuCores() * 2);
}


const std::string& RiakPushWorker::Context::AcquireUpstreamUrl() const {
  assert (upstream_urls.size() > 0);
  // get an unique upstream URL and set the pointer to the next one...
  const std::string& result = upstream_urls[next_upstream_url_];
  next_upstream_url_ = (next_upstream_url_ + 1) % upstream_urls.size();
  return result;
}


bool RiakPushWorker::DoGlobalInitialization(const Context *context) {
  // initiialize cURL
  if (curl_global_init(CURL_GLOBAL_ALL) != CURLE_OK) {
    LogCvmfs(kLogSpooler, kLogWarning, "Failed to initialize cURL library");
    return false;
  }

  // check for the presense of Riak upstream URLs
  if (context->upstream_urls.size() == 0) {
    LogCvmfs(kLogSpooler, kLogWarning, "No upstream URLs found.");
    return false;
  }

  if (!CheckRiakConfiguration(context)) {
    LogCvmfs(kLogSpooler, kLogWarning, "Riak configuration could not be "
                                       "validated!");
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
  compression_buffer_(NULL),
  compression_buffer_size_(context->compression_buffer_size),
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
  compression_buffer_ = (unsigned char*)malloc(compression_buffer_size_);
  assert (compression_buffer_ != NULL);
}


RiakPushWorker::~RiakPushWorker() {
  curl_easy_cleanup(curl_upload_);
  curl_easy_cleanup(curl_download_);
  curl_slist_free_all(http_headers_download_);

  free(compression_buffer_);

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
  if (curl_easy_setopt(curl_download_, CURLOPT_HEADERFUNCTION, &ObtainVclockCallback) != CURLE_OK)
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

//#define RANDOM_TEST

//#define NOOP
//#define READ_ONLY
//#define COMPRESSION_TO_MEM
//#define COMPRESSION_TO_DISK
//#define UPLOAD_RANDOM_DATA

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
  static const std::string tmp_dir = "/ramdisk/tmp";
  std::string tmp_file_path;
  content_hash = hash::Any(hash::kSha1);
  size_t bytes_used;

  struct stat st;
  stat(local_path.c_str(), &st);

#ifdef NOOP
  content_hash = hash::Any::randomHash(hash::kSha1);
  compression_job->Finished();

  return;
#endif

#ifdef READ_ONLY
  FILE *fr = fopen(local_path.c_str(), "r");
  const size_t array_size = 1024;
  unsigned char *buffer[array_size];

  while (!feof(fr)) {
    fread((void*)buffer, array_size, 1, fr);
  }

  fclose(fr);

  content_hash = hash::Any::randomHash(hash::kSha1);
  compression_job->Finished();

  return;
#endif

#ifdef COMPRESSION_TO_MEM
  if (! zlib::CompressPath2Mem(local_path,
                               compression_buffer_,
                               compression_buffer_size_,
                              &bytes_used,
                              &content_hash)) {
    abort();
  }

  content_hash = hash::Any::randomHash(hash::kSha1);
  compression_job->Finished();

  return;
#endif

#ifdef COMPRESSION_TO_DISK
  if (! CompressToTempFile(local_path,
                           tmp_dir,
                          &tmp_file_path,
                          &content_hash) ) {
    abort();
  }

  unlink(tmp_file_path.c_str());

  content_hash = hash::Any::randomHash(hash::kSha1);
  compression_job->Finished();

  return;
#endif

#ifdef UPLOAD_RANDOM_DATA
  content_hash = hash::Any::randomHash(hash::kSha1);
  bytes_used = std::min((size_t)st.st_size, compression_buffer_size_);
  PushMemoryToRiak(GenerateRiakKey(compression_job), compression_buffer_, bytes_used);

  compression_job->Finished();
  return;
#endif

  compression_stopwatch_.Start();

  if (! zlib::CompressPath2Mem(local_path,
                               compression_buffer_,
                               compression_buffer_size_,
                              &bytes_used,
                              &content_hash)) {
    LogCvmfs(kLogSpooler, kLogVerboseMsg, "In-Memory compression failed for file %s. "
                                          "Trying compression to disk... ",
             local_path.c_str());
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
  }

  compression_stopwatch_.Stop();

  // push to Riak
  upload_stopwatch_.Start();
  const std::string key = GenerateRiakKey(compression_job);
  const int retval = (tmp_file_path.empty()) ?
                        PushMemoryToRiak(key, compression_buffer_, bytes_used)
                                             :
                        PushFileToRiak(key, tmp_file_path);
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
size_t RiakPushWorker::ObtainVclockCallback(void *ptr, 
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
  if (!ConfigureUpload(key, url, headers, file_info.st_size, NULL, hd_src)) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to configure the upload CURL "
                                      "handle for file %s to Node: %s",
             file_path.c_str(), url.c_str());
    retcode = 3;
  }

  // do the actual business
  res = curl_easy_perform(curl_upload_);
  if (res != CURLE_OK) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to upload %s to Riak node %s "
                                      "because: '%s'",
             file_path.c_str(), url.c_str(), curl_easy_strerror(res));
    retcode = 4;
    goto out;
  }

  // check if all went fine
  if (!CheckUploadSuccess(file_info.st_size)) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to upload file %s to Riak node: "
                                      "%s",
             file_path.c_str(), url.c_str());
    retcode = 5;
    goto out;
  }

  if (!CollectUploadStatistics()) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to grab statistics data.");
    retcode = 6;
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


struct BufferWrapper {
  BufferWrapper(const unsigned char *data, size_t bytes) :
    data(data),
    bytes_to_read(bytes),
    offset(0) {}

  const unsigned char *data;
  const size_t         bytes_to_read;
  size_t               offset;
};


int RiakPushWorker::PushMemoryToRiak(const std::string   &key,
                                     const unsigned char *mem,
                                     const size_t         size,
                                     const bool           is_critical) {
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "pushing memory to Riak using key %s",
           key.c_str());

  const std::string url = CreateRequestUrl(key, is_critical);
  BufferWrapper buffer(mem, size);

  CURLcode res;
  struct curl_slist *headers = NULL;
  int      retcode = -1;

  // configure headers
  headers = curl_slist_append(headers, "Content-Type: application/octet-stream");
  if (!ConfigureUpload(key, url, headers, size, &WriteMemoryCallback, &buffer)) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to configure the upload CURL "
                                      "handle for memory upload to Node: %s",
             url.c_str());
    retcode = 1;
  }

  // do the actual business
  res = curl_easy_perform(curl_upload_);
  if (res != CURLE_OK) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to upload memory to Riak node %s "
                                      "because: '%s'",
             url.c_str(), curl_easy_strerror(res));
    retcode = 2;
    goto out;
  }

  // check if all went fine
  if (!CheckUploadSuccess(size)) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to upload memory data to Riak "
                                      "node: %s",
             url.c_str());
    retcode = 3;
    goto out;
  }

  if (!CollectUploadStatistics()) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to grab statistics data.");
    retcode = 4;
    goto out;
  }

  // all went well...
  retcode = 0;

out:
  // close the uploaded file
  curl_slist_free_all(headers);
  return retcode;
}


size_t RiakPushWorker::WriteMemoryCallback(void *ptr,
                                           size_t size,
                                           size_t nmemb,
                                           void *userdata) {
  BufferWrapper *wrapper = (BufferWrapper*)userdata;

  const size_t bytes = std::min(size*nmemb, wrapper->bytes_to_read -
                                            wrapper->offset);
  memcpy(ptr, wrapper->data + wrapper->offset, bytes);
  wrapper->offset += bytes;

  return bytes;
}

bool RiakPushWorker::ConfigureUpload(const std::string   &key,
                                     const std::string   &url,
                                     struct curl_slist   *headers,
                                     const size_t         data_size, 
                                     const UploadCallback callback,
                                     const void*          userdata) {
  std::string vector_clock;

  // check if key already exists and find out about its current vector clock
  if (GetVectorClock(key, vector_clock))
    headers = curl_slist_append(headers, ("X-Riak-Vclock: " + vector_clock).c_str());

  // set headers
  if (curl_easy_setopt(curl_upload_, CURLOPT_HTTPHEADER, headers) != CURLE_OK)
    return false;

  // set url for Riak put command
  if (curl_easy_setopt(curl_upload_, CURLOPT_URL, url.c_str()) != CURLE_OK)
    return false;

  // set file size of the file to be uploaded
  if (curl_easy_setopt(curl_upload_, CURLOPT_INFILESIZE_LARGE,
                       (curl_off_t)data_size) != CURLE_OK)
    return false;

  // specify the read callback
  if (curl_easy_setopt(curl_upload_, CURLOPT_READFUNCTION, callback) != CURLE_OK)
    return false;

  // specify the file handle to be uploaded
  if (curl_easy_setopt(curl_upload_, CURLOPT_READDATA, userdata) != CURLE_OK)
    return false;

  return true;
}


bool RiakPushWorker::CheckUploadSuccess(const int file_size) {
  long response_code;
  double uploaded_bytes;

  if (curl_easy_getinfo(curl_upload_, CURLINFO_RESPONSE_CODE, &response_code) != CURLE_OK)
    return false;

  if (curl_easy_getinfo(curl_upload_, CURLINFO_SIZE_UPLOAD, &uploaded_bytes) != CURLE_OK)
    return false;

  if (response_code != 204 && response_code != 200)
    return false;

  if ((int)uploaded_bytes != file_size)
    return false;

  return true;
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


std::string RiakPushWorker::GenerateRandomKey() const {
  static const size_t random_key_length = 10;
  std::string result(random_key_length, ' ');
  for (size_t i = 0; i < random_key_length; ++i) {
    result[i] = static_cast<char>(rand() % 26 + static_cast<int>('a'));
  }
  return result;
}


std::string RiakPushWorker::CreateRequestUrl(const std::string &key,
                                             const bool is_critical) const {
  const std::string additional = is_critical ? "&w=all&dw=all" : "";
  return upstream_url_ + "/" + key + "?returnbody=false" + additional;
}


bool RiakPushWorker::CheckRiakConfiguration(const Context *context) {
  // get the URL for the configuration download
  assert (!context->upstream_urls.empty());
  const std::string url = context->upstream_urls.front();

  // download the configuration
  DataBuffer buffer;
  if (!DownloadRiakConfiguration(url, buffer)) return false;

  // parse JSON configuration
  JSON *root = ParseJsonConfiguration(buffer);

  // check the configuration
  return CheckJsonConfiguration(root);
}


bool RiakPushWorker::DownloadRiakConfiguration(const std::string &url,
                                               DataBuffer& buffer) {
  // initialize cURL handle
  CURL *curl_handle = curl_easy_init();
  if (! curl_handle) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to initialize cURL handle to "
                                      "check the riak configuration.");
    return false;
  }

  // set request headers
  struct curl_slist *headers = NULL;
  headers = curl_slist_append(headers, "Accept: */*");

  // configure cURL handle
  if (curl_easy_setopt(curl_handle, CURLOPT_NOPROGRESS, 1L)                      != CURLE_OK ||
      curl_easy_setopt(curl_handle, CURLOPT_TCP_KEEPALIVE, 0L)                   != CURLE_OK ||
      curl_easy_setopt(curl_handle, CURLOPT_HTTPGET, 1L)                         != CURLE_OK ||
      curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, headers)                 != CURLE_OK ||
      curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, &buffer)                  != CURLE_OK ||
      curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, &ReceiveDataCallback) != CURLE_OK ||
      curl_easy_setopt(curl_handle, CURLOPT_URL, url.c_str())          != CURLE_OK)
  {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to configure cURL handle to "
                                      "check the riak configuration.");
    return false;
  }

  // do the action!
  if (curl_easy_perform(curl_handle) != CURLE_OK) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to download bucket configuration "
                                      "for Riak URL: %s",
             url.c_str());
    return false;
  }

  // check the response code
  long response_code;
  if (curl_easy_getinfo(curl_handle, CURLINFO_RESPONSE_CODE, &response_code) != CURLE_OK) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to retrieve response code from "
                                      "request");
    return false;
  }

  // check if response code is valid
  if (response_code != 200) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to check configuration of Riak "
                                      "HTTP Response code was: %d",
             response_code);
    return false; 
  }

  // all done...
  curl_easy_cleanup(curl_handle);

  return true;
}


JSON* RiakPushWorker::ParseJsonConfiguration(DataBuffer& buffer) {
  // parse the JSON string with the vjson library
  char *error_pos  = 0;
  char *error_desc = 0;
  int error_line   = 0;
  block_allocator allocator(1 << 10);
  JSON *root = json_parse((char*)buffer.data,
                                &error_pos,
                                &error_desc,
                                &error_line,
                                &allocator);

  // check if the json string was parsed successfully
  if (!root) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to parse Riak configuration json "
                                      "string.\n"
                                      "Error at line %d: %s\n"
                                      "%s\n"
                                      "JSON String was:\n"
                                      "%s",
             error_line, error_desc, error_pos, buffer.data);
  }

  return root;
}

struct CheckResponse { // C++11: replace this scoped boxing by a typed enum!
  enum T {
    Correct,
    NotCorrect,
    NotMyResponsibility
  };
};

/**
 * checks a single property JSON object for a certain value
 * Note: you need to specialize this template for each data type you like to
 *       check! Please give a consistent error message, if you notice that some-
 *       thing is fishy with the configuration.
 *
 * @param object         the property JSON object to be checked
 * @param prop_name      the property name to look for
 * @param expectation    the expected value for the checked property
 * @return               Correct                if the right property was found
 *                                              to be correct
 *                       NotCorrect             if the right property was found
 *                                              but it is misconfigured
 *                       NotMyResponsibility    if we did not hit the right
 *                                              property
 */
template <typename T>
CheckResponse::T Check(const JSON *object,
                       const std::string &prop_name,
                       const T &expectation) {
  const bool not_implemented = false;
  assert (not_implemented);
}

template <>
CheckResponse::T Check<bool>(const JSON *object,
                             const std::string &prop_name,
                             const bool &expectation) {
  // check if we got the right object here
  if (object->type != JSON_BOOL    ||
      prop_name    != object->name)
    return CheckResponse::NotMyResponsibility;

  // check if the expected value is set
  if (object->int_value != expectation) {
    LogCvmfs(kLogSpooler, kLogStderr, "Expected Riak config '%s' to be %s but "
                                      "turned out to be %s.",
             prop_name.c_str(), (expectation ? "true" : "false"),
                                (object->int_value ? "true" : "false"));
    return CheckResponse::NotCorrect;
  }

  // all good
  return CheckResponse::Correct;
}

template <typename T>
bool ConfigAssertion(const JSON *object,
                     const std::string &prop_name,
                     const T &expectation) {
  // go through the configuration and check for the right value
  for (JSON *it = object->first_child; it != NULL; it = it->next_sibling) {
    CheckResponse::T result = Check<T>(it, prop_name, expectation);
    if (result == CheckResponse::Correct)    return true;
    if (result == CheckResponse::NotCorrect) return false;
  }

  // element was not found in the configuration... this is fishy!
  LogCvmfs(kLogSpooler, kLogStderr, "No entry for '%s' found in the given Riak "
                                    "configuration JSON object.",
           prop_name.c_str());
  return false;
}


bool RiakPushWorker::CheckJsonConfiguration(const JSON *json_root) {
  // check general structure of JSON configuration
  if (json_root                    == NULL               ||
      json_root->type              != JSON_OBJECT        ||
      json_root->first_child       == NULL               ||
      json_root->first_child->type != JSON_OBJECT        ||
      strcmp(json_root->first_child->name, "props") != 0) {
    LogCvmfs(kLogSpooler, kLogStderr, "Cannot read JSON configuration returned "
                                      "by Riak.");
    return false;
  }

  // check individual Riak configurations
  JSON *props = json_root->first_child;
  return ConfigAssertion<bool>(props, "allow_mult",      false) &&
         ConfigAssertion<bool>(props, "last_write_wins", true);
}


size_t RiakPushWorker::ReceiveDataCallback(void *ptr,
                                           size_t size,
                                           size_t nmemb,
                                           void *userdata) {
  const size_t bytes = size * nmemb;
  DataBuffer& buffer = *(static_cast<DataBuffer*>(userdata));

  if (!buffer.Reserve(bytes)) {
    return 0;
  }

  buffer.Copy((unsigned char*)ptr, bytes);

  return bytes;
}


bool RiakPushWorker::RiakPushWorker::IsReady() const {
  const bool ready = AbstractPushWorker::IsReady();
  return ready && initialized_;
}


// -----------------------------------------------------------------------------


bool RiakPushWorker::DataBuffer::Reserve(const size_t bytes) {
  unsigned char *new_buffer = (unsigned char*)realloc((void*)data,
                                                             size_ + bytes);
  if (new_buffer == NULL)
    return false;

  data   = new_buffer;
  size_ += bytes;
  return true;
}


unsigned char* RiakPushWorker::DataBuffer::Position() const {
  return data + offset_;
}


void RiakPushWorker::DataBuffer::Copy(const unsigned char* ptr,
                                      const size_t bytes) {
  const size_t free_space = size_ - offset_;
  assert (free_space >= bytes);

  memcpy(Position(), ptr, bytes);
  offset_ += bytes;
}
