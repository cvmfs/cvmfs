// /**
//  * This file is part of the CernVM File System.
//  */

// #include "upload_riak.h"

// #include <fcntl.h>
// #include <sys/stat.h>
// #include <unistd.h>
// #include <stdlib.h>

// #include <algorithm>

// #include "duplex_curl.h"
// #include "logging.h"
// #include "compression.h"

// #include "util.h"
// #include "util_concurrency.h"
// #include "vjson/json.h"

// using namespace upload;


// const std::string& RiakSpooler::UploadWorker::worker_context::AcquireUpstreamUrl() const {
//   assert (upstream_urls.size() > 0);
//   // get an unique upstream URL and set the pointer to the next one...
//   LockGuard<worker_context> guard(this);
//   const std::string& result = upstream_urls[next_upstream_url_];
//   next_upstream_url_ = (next_upstream_url_ + 1) % upstream_urls.size();
//   return result;
// }


// RiakSpooler::RiakSpooler(const SpoolerDefinition &spooler_definition) :
//   AbstractSpooler(spooler_definition),

//   concurrent_compression_(NULL),
//   concurrent_upload_(NULL),
//   compression_context_(NULL),
//   upload_context_(NULL)
// {
//   assert (spooler_definition.IsValid() &&
//           spooler_definition.driver_type == SpoolerDefinition::Riak);
// }



// bool RiakSpooler::Initialize() {
//   // get the individual upstream URLs out of the spooler description string
//   std::vector<std::string> upstream_url_vector =
//     SplitString(spooler_definition().spooler_description, '@');
//   if (upstream_url_vector.empty()) {
//     LogCvmfs(kLogSpooler, kLogWarning, "No Riak upstream URL given");
//     return false;
//   }

//   // initialize the cURL environment
//   if (curl_global_init(CURL_GLOBAL_ALL) != CURLE_OK) {
//     LogCvmfs(kLogSpooler, kLogWarning, "Failed to initialize cURL library");
//     return false;
//   }

//   // check if the Riak cluster is correctly configured
//   if (! CheckRiakConfiguration(upstream_url_vector.front())) {
//     LogCvmfs(kLogSpooler, kLogWarning, "Riak cluster is misconfigured");
//     return false;
//   }

//   // generate the concurrent worker contexts
//   const std::string &temp_directory = spooler_definition().temp_directory;
//   compression_context_ = new CompressionWorker::worker_context(temp_directory);
//   upload_context_      = new UploadWorker::worker_context(upstream_url_vector);

//   assert (compression_context_ && upload_context_);

//   // create the concurrent workers environments
//   const unsigned int number_of_cpus = GetNumberOfCpuCores();
//   concurrent_compression_ =
//     new ConcurrentWorkers<CompressionWorker>(number_of_cpus,
//                                              number_of_cpus * 500, // TODO: magic number (?)
//                                              compression_context_);
//   concurrent_upload_      =
//     new ConcurrentWorkers<UploadWorker>(number_of_cpus * 5,
//                                         number_of_cpus * 500, // TODO: magic number (?)
//                                         upload_context_);

//   assert(concurrent_compression_ && concurrent_upload_);

//   // initialize the concurrent workers
//   if (! concurrent_compression_->Initialize() ||
//       ! concurrent_upload_->Initialize()) {
//     LogCvmfs(kLogSpooler, kLogWarning, "Failed to initialize concurrent "
//                                        "workers for RiakSpooler.");
//     return false;
//   }

//   // register callbacks to the concurrent workers
//   concurrent_compression_->RegisterListener(
//                               &RiakSpooler::CompressionWorkerCallback,
//                               this);
//   concurrent_upload_->RegisterListener(
//                               &RiakSpooler::JobDone,
//                               this);

//   // all set... ready to go
//   return true;
// }


// void RiakSpooler::TearDown() {
//   concurrent_compression_->WaitForTermination();
//   concurrent_upload_->WaitForTermination();

//   curl_global_cleanup();
// }


// RiakSpooler::~RiakSpooler() {
//   //
//   // Legacy code: readout of the performance counters...
//   //              this might be used as a simple template later on

//   // const double uploaded_jobs = (double)upload_jobs_count_;
//   // LogCvmfs(kLogSpooler, kLogVerboseMsg, "Statistics:\n"
//   //                                       "Avg Compression time:          %f s\n"
//   //                                       "Avg Uploading time:            %f s\n"
//   //                                       "Avg CURL upload time:          %f s\n"
//   //                                       "Avg CURL Vclock retrieve time: %f s\n"
//   //                                       "Avg CURL connection time:      %f s\n"
//   //                                       "CURL connections:              %d\n"
//   //                                       "Avg Data Upload speed:         %f kB/s",

//   //          (compression_time_aggregated_     / uploaded_jobs),
//   //          (upload_time_aggregated_          / uploaded_jobs),
//   //          (curl_upload_time_aggregated_     / uploaded_jobs),
//   //          (curl_get_vclock_time_aggregated_ / uploaded_jobs),
//   //          (curl_connection_time_aggregated_ / uploaded_jobs*2),
//   //           curl_connections_,
//   //          (curl_upload_speed_aggregated_    / uploaded_jobs / 1024.0));
// }


// void RiakSpooler::Copy(const std::string &local_path,
//                        const std::string &remote_path) {
//   // schedule a plain upload job into the UploadWorker. In this case no com=
//   // pression is performed
//   upload_parameters input(local_path, remote_path, move());
//   concurrent_upload_->Schedule(input);
// }


// void RiakSpooler::ProcessChunk(const std::string   &local_path,
//                                const std::string   &remote_dir,
//                                const unsigned long  offset,
//                                const unsigned long  length) {
//   // schedule a compression job into the CompressionWorker
//   // Note: on successful completion this will schedule an additional job
//   //       into the UploadWorker in order to push the compression (and
//   //       hashing) results into the Riak storage
//   //       See: CompressionWorkerCallback()
//   compression_parameters params(local_path, remote_dir, "", move());
//   concurrent_compression_->Schedule(params);
// }


// void RiakSpooler::CompressionWorkerCallback(
//                     const RiakSpooler::CompressionWorker::returned_data &data) {
//   // checks if the compression job was successful and schedules an upload job
//   // into the UploadWorker.
//   // Note: If the compression job failed it will be directly presented to the
//   //       user without attemting an upload
//   if (data.type != CompressionWorker::returned_data::kEmpty) {
//     concurrent_upload_->Schedule(data);
//   } else {
//     JobDone(data);
//   }
// }


// void RiakSpooler::EndOfTransaction() {
//   LogCvmfs(kLogSpooler, kLogVerboseMsg, "End of Transaction");
//   WaitForUpload();
// }


// void RiakSpooler::WaitForUpload() const {
//   LogCvmfs(kLogSpooler, kLogVerboseMsg, "Wait for Upload");
//   concurrent_compression_->WaitForEmptyQueue();
//   concurrent_upload_->WaitForEmptyQueue();
// }


// void RiakSpooler::WaitForTermination() const {
//   LogCvmfs(kLogSpooler, kLogVerboseMsg, "Wait for Termination");
//   concurrent_compression_->WaitForTermination();
//   concurrent_upload_->WaitForTermination();
// }


// unsigned int RiakSpooler::GetNumberOfErrors() const {
//   return concurrent_compression_->GetNumberOfFailedJobs() +
//          concurrent_upload_->GetNumberOfFailedJobs();
// }


// //
// // +----------------------------------------------------------------------------
// // | Compression Worker
// //


// RiakSpooler::CompressionWorker::CompressionWorker(
//                 const RiakSpooler::CompressionWorker::worker_context *context) :
//   compression_time_aggregated_(0),
//   temp_directory_(context->temp_directory)
// {}


// void RiakSpooler::CompressionWorker::operator()(const expected_data &input) {
//   // output variables for CompressToTempFile()
//   std::string tmp_file_path;
//   hash::Any   content_hash(hash::kSha1);

//   //  compress the file to a temporary location and get timing information
//   compression_stopwatch_.Reset();
//   compression_stopwatch_.Start();
//   const bool success = CompressToTempFile(input.local_path,
//                                           temp_directory_,
//                                          &tmp_file_path,
//                                          &content_hash);
//   compression_stopwatch_.Stop();
//   compression_time_aggregated_ += compression_stopwatch_.GetTime();

//   // check if all went well and inform the controller above
//   if (! success) {
//     LogCvmfs(kLogSpooler, kLogWarning, "Failed to compress file before pushing "
//                                        "to Riak: %s",
//              input.local_path.c_str());

//     master()->JobFailed(returned_data(101, input.local_path));
//   } else {
//     master()->JobSuccessful(returned_data(0,
//                                           input.local_path,
//                                           tmp_file_path,
//                                           input.remote_dir,
//                                           content_hash,
//                                           input.file_suffix,
//                                           input.move));
//   }
// }


// bool RiakSpooler::CompressionWorker::CompressToTempFile(
//                                         const std::string &source_file_path,
//                                         const std::string &destination_dir,
//                                         std::string       *tmp_file_path,
//                                         hash::Any         *content_hash) const {
//   // Create a temporary file at the given destination directory
//   FILE *fcas = CreateTempFile(destination_dir + "/cvmfs", 0777, "w",
//                               tmp_file_path);
//   if (fcas == NULL) {
//     LogCvmfs(kLogSpooler, kLogStderr, "failed to create temporary file %s",
//              tmp_file_path->c_str());
//     return false;
//   }

//   // Compress the provided source file and write the result into the temporary.
//   // Additionally computes the content hash of the compressed data
//   int retval = zlib::CompressPath2File(source_file_path, fcas, content_hash);
//   if (! retval) {
//     LogCvmfs(kLogSpooler, kLogStderr, "failed to compress file %s to temporary "
//                                       "file %s",
//              source_file_path.c_str(), tmp_file_path->c_str());

//     unlink(tmp_file_path->c_str());
//     return false;
//   }
//   fclose(fcas);

//   // all done...
//   return true;
// }



// //
// // +----------------------------------------------------------------------------
// // | Upload Worker
// //


// RiakSpooler::UploadWorker::UploadWorker(
//                      const RiakSpooler::UploadWorker::worker_context *context) :
//   upstream_url_(context->AcquireUpstreamUrl()),
//   http_headers_download_(NULL),

//   upload_time_aggregated_(0),
//   curl_upload_time_aggregated_(0),
//   curl_get_vclock_time_aggregated_(0),
//   curl_connection_time_aggregated_(0),
//   curl_connections_(0),
//   curl_upload_speed_aggregated_(0)
// {}


// bool RiakSpooler::UploadWorker::Initialize() {
//   LogCvmfs(kLogSpooler, kLogVerboseMsg, "Configuring cURL handles for putting "
//                                         "files into a Riak instance");

//   // initialize the cURL handles
//   if (!InitUploadHandle() || !InitDownloadHandle()) {
//     LogCvmfs(kLogSpooler, kLogStderr, "failed to initialize cURL handle.");
//     return false;
//   }

//   // all done
//   LogCvmfs(kLogSpooler, kLogVerboseMsg, "successfully initialized Riak "
//                                         "spooler backend");
//   return true;
// }


// void RiakSpooler::UploadWorker::TearDown() {
//   curl_easy_cleanup(curl_upload_);
//   curl_easy_cleanup(curl_download_);
//   curl_slist_free_all(http_headers_download_);
// }


// bool RiakSpooler::UploadWorker::InitUploadHandle() {
//   // initialize cURL handle
//   curl_upload_ = curl_easy_init();
//   if (! curl_upload_) {
//     LogCvmfs(kLogSpooler, kLogStderr, "failed to initialize cURL upload "
//                                       "handle.");
//     return false;
//   }

//   // configure cURL handle
//   if (curl_easy_setopt(curl_upload_, CURLOPT_NOPROGRESS, 1L)    != CURLE_OK)
//     return false;
//   if (curl_easy_setopt(curl_upload_, CURLOPT_TCP_KEEPALIVE, 1L) != CURLE_OK)
//     return false;
//   if (curl_easy_setopt(curl_upload_, CURLOPT_UPLOAD, 1L)        != CURLE_OK)
//     return false;

//   return true;
// }


// bool RiakSpooler::UploadWorker::InitDownloadHandle() {
//   // initialize cURL handle
//   curl_download_ = curl_easy_init();
//   if (! curl_download_) {
//     LogCvmfs(kLogSpooler, kLogStderr, "failed to initialize cURL download "
//                                       "handle.");
//     return false;
//   }

//   // configure cURL handle
//   if (curl_easy_setopt(curl_download_, CURLOPT_NOPROGRESS, 1L)    != CURLE_OK)
//     return false;
//   if (curl_easy_setopt(curl_download_, CURLOPT_TCP_KEEPALIVE, 1L) != CURLE_OK)
//     return false;
//   if (curl_easy_setopt(curl_download_, CURLOPT_HTTPGET, 1L)       != CURLE_OK)
//     return false;
//   if (curl_easy_setopt(curl_download_, CURLOPT_NOBODY, 1L)        != CURLE_OK)
//     return false;

//   // configure download headers
//   http_headers_download_ = curl_slist_append(http_headers_download_, "Accept: */*");
//   if (curl_easy_setopt(curl_download_, CURLOPT_HTTPHEADER, http_headers_download_) != CURLE_OK)
//     return false;

//   // configure header readout callback
//   if (curl_easy_setopt(curl_download_, CURLOPT_HEADERFUNCTION, &ObtainVclockCallback) != CURLE_OK)
//     return false;

//   return true;
// }


// size_t RiakSpooler::UploadWorker::ObtainVclockCallback(void *ptr,
//                                                        size_t size,
//                                                        size_t nmemb,
//                                                        void *userdata) {
//   // vector clock header description
//   // Example:
//   // X-Riak-Vclock: a85hYGBgzGDKBVIceQ1fzgWYubNkMCUy5rEybNMzOcWXBQA=\r\n
//   // ~~~ name ~~~~  ~~~~~~~~~~~~~~~~~~~ clock ~~~~~~~~~~~~~~~~~~~~~~
//   //              ^~ padding             carriage return / line feed ~^
//   static const char*  vclock_header_name        = "X-Riak-Vclock";
//   static const size_t vclock_header_name_length = strlen(vclock_header_name);
//   static const size_t vclock_header_padding     = 2;
//   static const size_t vclock_header_newline     = 2;

//   // incoming data shaping
//   const char   *bytes          = (const char*)ptr;
//   const size_t  bytes_received = size*nmemb;
//   std::string  &result         = *(reinterpret_cast<std::string*>(userdata));

//   // check for the vector clock header
//   if (strncmp(bytes,
//               vclock_header_name,
//               std::min(bytes_received, vclock_header_name_length)) == 0)
//   {
//     // compute the length and position of the vector clock data
//     const char* start_of_clock_value = bytes                     +
//                                        vclock_header_name_length +
//                                        vclock_header_padding;
//     const size_t vector_clock_length = bytes_received            -
//                                        vclock_header_name_length -
//                                        vclock_header_padding     -
//                                        vclock_header_newline;

//     // copy the vector clock data into the result string
//     result = std::string(start_of_clock_value, vector_clock_length);
//   }

//   // cURL needs to receive the exact number of incoming bytes as return value
//   return bytes_received;
// }


// bool RiakSpooler::UploadWorker::GetVectorClock(const std::string &key,
//                                                std::string &vector_clock) {
//   LogCvmfs(kLogSpooler, kLogVerboseMsg, "checking if key %s already exists",
//            key.c_str());

//   // generate the request URL
//   const std::string url = CreateRequestUrl(key);
//   if (curl_easy_setopt(curl_download_, CURLOPT_URL, url.c_str()) != CURLE_OK)
//     return false;

//   // set the vector_clock to be written in the ReadHeaderCallback()
//   if (curl_easy_setopt(curl_download_, CURLOPT_WRITEHEADER, (void*)&vector_clock) != CURLE_OK)
//     return false;

//   // do the actual business
//   CURLcode res = curl_easy_perform(curl_download_);
//   if (res != CURLE_OK) {
//     LogCvmfs(kLogSpooler, kLogStderr, "Failed to check existence of key %s in "
//                                       "Riak because: '%s'",
//              key.c_str(), curl_easy_strerror(res));
//     return false;
//   }

//   // check if all went fine
//   long response_code;
//   res = curl_easy_getinfo(curl_download_, CURLINFO_RESPONSE_CODE, &response_code);
//   if (res != CURLE_OK) {
//     LogCvmfs(kLogSpooler, kLogStderr, "Unable to retrieve response code for "
//                                       "key %s in Riak node %s",
//              key.c_str(), url.c_str());
//     return false;
//   }

//   // check if object was found
//   if (response_code != 200 && response_code != 304)
//     return false;

//   if (!CollectVclockFetchStatistics()) {
//     LogCvmfs(kLogSpooler, kLogStderr, "Failed to collect vclock fetch statistics");
//     return false;
//   }

//   // we found an object and therefore most likely vector_clock is set now
//   return true;
// }


// bool RiakSpooler::UploadWorker::CollectVclockFetchStatistics() {
//   CURLcode res;
//   double request_time;
//   double connection_time;

//   res = curl_easy_getinfo(curl_download_, CURLINFO_TOTAL_TIME, &request_time);
//   if (res != CURLE_OK) return false;
//   curl_get_vclock_time_aggregated_ += request_time;

//   res = curl_easy_getinfo(curl_download_, CURLINFO_CONNECT_TIME, &connection_time);
//   if (res != CURLE_OK) return false;
//   curl_connection_time_aggregated_ += connection_time;

//   return true;
// }


// void RiakSpooler::UploadWorker::operator()(const expected_data &input) {
//   assert (input.type != expected_data::kEmpty);

//   if (input.move) {
//     LogCvmfs(kLogSpooler, kLogStderr, "RiakSpooler does not support move at "
//                                       "the moment.");
//     master()->JobFailed(SpoolerResult(1,
//                                       input.local_path,
//                                       input.content_hash));
//     return;
//   }

//   // push to Riak
//   upload_stopwatch_.Reset();
//   upload_stopwatch_.Start();
//   const std::string key = input.GetRiakKey();
//   const int retval = PushFileToRiak(key,
//                                     input.upload_source_path,
//                                     (input.type == expected_data::kPlainUpload));
//   upload_stopwatch_.Stop();
//   upload_time_aggregated_ += upload_stopwatch_.GetTime();

//   // clean up
//   if (input.type == expected_data::kCompressedUpload) {
//     unlink(input.upload_source_path.c_str());
//   }

//   // return results to the controller
//   SpoolerResult return_value(retval,
//                              input.local_path,
//                              input.content_hash);
//   if (retval != 0) {
//     LogCvmfs(kLogSpooler, kLogWarning, "Failed to upload file '%s' to Riak node "
//                                        "'%s' using key '%s'",
//              input.local_path.c_str(),
//              upstream_url_.c_str(),
//              key.c_str());
//     master()->JobFailed(return_value);
//   } else {
//     master()->JobSuccessful(return_value);
//   }
// }


// std::string RiakSpooler::UploadWorker::CreateRequestUrl(
//                                               const std::string &key,
//                                               const bool is_critical) const {
//   // configure the upload URL for performance of consistency
//   const std::string additional = is_critical ? "&w=all&dw=all" : "&w=1";

//   // build up the upstream URL
//   return upstream_url_ + "/" + key + "?returnbody=false" + additional;
// }


// int RiakSpooler::UploadWorker::PushFileToRiak(const std::string &key,
//                                               const std::string &file_path,
//                                               const bool         is_critical) {
//   LogCvmfs(kLogSpooler, kLogVerboseMsg, "pushing file %s to Riak using key %s",
//            file_path.c_str(), key.c_str());

//   // get context information
//   const std::string url = CreateRequestUrl(key, is_critical);
//   FILE *hd_src;

//   CURLcode res;
//   struct curl_slist *headers = NULL;
//   int      retcode = -1;

//   std::string vector_clock;

//   // find the size of the file to be uploaded
//   struct stat file_info;
//   int hd = open(file_path.c_str(), O_RDONLY);
//   if (hd < 0) {
//     LogCvmfs(kLogSpooler, kLogStderr, "Failed to stat file %s",
//              file_path.c_str());
//     retcode = 1;
//     close(hd);
//     goto out;
//   }
//   fstat(hd, &file_info);
//   close(hd);

//   // open the file for reading
//   hd_src = fopen(file_path.c_str(), "rb");
//   if (hd_src == NULL) {
//     LogCvmfs(kLogSpooler, kLogStderr, "Failed to open file %s for reading.",
//              file_path.c_str());
//     retcode = 2;
//     goto out;
//   }

//   // configure headers
//   headers = curl_slist_append(headers, "Content-Type: application/octet-stream");
//   if (!ConfigureUpload(key, url, headers, file_info.st_size, NULL, hd_src)) {
//     LogCvmfs(kLogSpooler, kLogStderr, "Failed to configure the upload CURL "
//                                       "handle for file %s to Node: %s",
//              file_path.c_str(), url.c_str());
//     retcode = 3;
//   }

//   // do the actual business
//   res = curl_easy_perform(curl_upload_);
//   if (res != CURLE_OK) {
//     LogCvmfs(kLogSpooler, kLogStderr, "Failed to upload %s to Riak node %s "
//                                       "because: '%s'",
//              file_path.c_str(), url.c_str(), curl_easy_strerror(res));
//     retcode = 4;
//     goto out;
//   }

//   // check if all went fine
//   if (!CheckUploadSuccess(file_info.st_size)) {
//     LogCvmfs(kLogSpooler, kLogStderr, "Failed to upload file %s to Riak node: "
//                                       "%s",
//              file_path.c_str(), url.c_str());
//     retcode = 5;
//     goto out;
//   }

//   if (!CollectUploadStatistics()) {
//     LogCvmfs(kLogSpooler, kLogStderr, "Failed to grab statistics data.");
//     retcode = 6;
//     goto out;
//   }

//   // all went well...
//   retcode = 0;

// out:
//   // clean up
//   fclose(hd_src);
//   curl_slist_free_all(headers);
//   return retcode;
// }

// bool RiakSpooler::UploadWorker::ConfigureUpload(const std::string   &key,
//                                                 const std::string   &url,
//                                                 struct curl_slist   *headers,
//                                                 const size_t         data_size,
//                                                 const UploadCallback callback,
//                                                 const void*          userdata) {
//   std::string vector_clock;

//   // check if key already exists and find out about its current vector clock
//   // if (GetVectorClock(key, vector_clock))
//   //   headers = curl_slist_append(headers, ("X-Riak-Vclock: " + vector_clock).c_str());

//   // set headers
//   if (curl_easy_setopt(curl_upload_, CURLOPT_HTTPHEADER, headers) != CURLE_OK)
//     return false;

//   // set url for Riak put command
//   if (curl_easy_setopt(curl_upload_, CURLOPT_URL, url.c_str()) != CURLE_OK)
//     return false;

//   // set file size of the file to be uploaded
//   if (curl_easy_setopt(curl_upload_, CURLOPT_INFILESIZE_LARGE,
//                        (curl_off_t)data_size) != CURLE_OK)
//     return false;

//   // specify the read callback
//   if (curl_easy_setopt(curl_upload_, CURLOPT_READFUNCTION, callback) != CURLE_OK)
//     return false;

//   // specify the file handle to be uploaded
//   if (curl_easy_setopt(curl_upload_, CURLOPT_READDATA, userdata) != CURLE_OK)
//     return false;

//   return true;
// }


// bool RiakSpooler::UploadWorker::CheckUploadSuccess(const int file_size) {
//   long response_code;
//   double uploaded_bytes;

//   // get HTTP response code
//   if (curl_easy_getinfo(curl_upload_, CURLINFO_RESPONSE_CODE, &response_code) != CURLE_OK)
//     return false;
//   if (response_code != 204 && response_code != 200)
//     return false;

//   // check number of uploaded bytes
//   if (curl_easy_getinfo(curl_upload_, CURLINFO_SIZE_UPLOAD, &uploaded_bytes) != CURLE_OK)
//     return false;
//   if ((int)uploaded_bytes != file_size)
//     return false;

//   // all fine...
//   return true;
// }


// bool RiakSpooler::UploadWorker::CollectUploadStatistics() {
//   CURLcode res;

//   double upload_time;
//   double connection_time;
//   long   connections;
//   double upload_speed;

//   res = curl_easy_getinfo(curl_upload_, CURLINFO_TOTAL_TIME, &upload_time);
//   if (res != CURLE_OK) return false;
//   curl_upload_time_aggregated_ += upload_time;

//   res = curl_easy_getinfo(curl_upload_, CURLINFO_CONNECT_TIME, &connection_time);
//   if (res != CURLE_OK) return false;
//   curl_connection_time_aggregated_ += connection_time;

//   res = curl_easy_getinfo(curl_upload_, CURLINFO_NUM_CONNECTS, &connections);
//   if (res != CURLE_OK) return false;
//   curl_connections_ += connections;

//   res = curl_easy_getinfo(curl_upload_, CURLINFO_SPEED_UPLOAD, &upload_speed);
//   if (res != CURLE_OK) return false;
//   curl_upload_speed_aggregated_ += upload_speed;

//   return true;
// }

// //
// // +----------------------------------------------------------------------------
// // | Auxiliary
// //


// std::string RiakSpooler::upload_parameters::GetRiakKey() const {
//   if (type == upload_parameters::kCompressedUpload) {
//     // generate Riak key from the content hash
//     return remote_dir              +
//            content_hash.ToString() +
//            file_suffix;

//   } else if (type == upload_parameters::kPlainUpload) {
//     // remove slashes from the remote_path (Riak cannot handle them in keys)
//     std::string result;
//     std::remove_copy(remote_path.begin(),
//                      remote_path.end(),
//                      std::back_inserter(result),
//                      '/');
//     return result;

//   } else {
//     // something went terribly wrong here!
//     const bool malformed_upload_parameters = false;
//     assert (malformed_upload_parameters);
//   }
// }


// bool RiakSpooler::CheckRiakConfiguration(const std::string &url) {
//   // download the configuration
//   DataBuffer buffer;
//   if (! DownloadRiakConfiguration(url, buffer)) return false;

//   // parse JSON configuration
//   JSON *root = ParseJsonConfiguration(buffer);

//   // check the configuration
//   return CheckJsonConfiguration(root);
// }


// /// Data callback for Configuration Retrieval
// size_t ReceiveDataCallback(void *ptr,
//                            size_t size,
//                            size_t nmemb,
//                            void *userdata) {
//   const size_t bytes = size * nmemb;
//   RiakSpooler::DataBuffer& buffer =
//                             *(static_cast<RiakSpooler::DataBuffer*>(userdata));

//   if (!buffer.Reserve(bytes)) {
//     return 0;
//   }

//   buffer.Copy((unsigned char*)ptr, bytes);
//   return bytes;
// }


// bool RiakSpooler::DownloadRiakConfiguration(const std::string &url,
//                                             DataBuffer& buffer) {
//   // initialize cURL handle
//   CURL *curl_handle = curl_easy_init();
//   if (! curl_handle) {
//     LogCvmfs(kLogSpooler, kLogStderr, "Failed to initialize cURL handle to "
//                                       "check the riak configuration.");
//     return false;
//   }

//   // set request headers
//   struct curl_slist *headers = NULL;
//   headers = curl_slist_append(headers, "Accept: */*");

//   // configure cURL handle
//   if (curl_easy_setopt(curl_handle, CURLOPT_NOPROGRESS, 1L)                      != CURLE_OK ||
//       curl_easy_setopt(curl_handle, CURLOPT_TCP_KEEPALIVE, 0L)                   != CURLE_OK ||
//       curl_easy_setopt(curl_handle, CURLOPT_HTTPGET, 1L)                         != CURLE_OK ||
//       curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, headers)                 != CURLE_OK ||
//       curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, &buffer)                  != CURLE_OK ||
//       curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, &ReceiveDataCallback) != CURLE_OK ||
//       curl_easy_setopt(curl_handle, CURLOPT_URL, url.c_str())          != CURLE_OK)
//   {
//     LogCvmfs(kLogSpooler, kLogStderr, "Failed to configure cURL handle to "
//                                       "check the riak configuration.");
//     return false;
//   }

//   // do the action!
//   if (curl_easy_perform(curl_handle) != CURLE_OK) {
//     LogCvmfs(kLogSpooler, kLogStderr, "Failed to download bucket configuration "
//                                       "for Riak URL: %s",
//              url.c_str());
//     return false;
//   }

//   // check the response code
//   long response_code;
//   if (curl_easy_getinfo(curl_handle, CURLINFO_RESPONSE_CODE, &response_code) != CURLE_OK) {
//     LogCvmfs(kLogSpooler, kLogStderr, "Failed to retrieve response code from "
//                                       "request");
//     return false;
//   }

//   // check if response code is valid
//   if (response_code != 200) {
//     LogCvmfs(kLogSpooler, kLogStderr, "Failed to check configuration of Riak "
//                                       "HTTP Response code was: %d",
//              response_code);
//     return false;
//   }

//   // cleanup and return
//   curl_easy_cleanup(curl_handle);
//   return true;
// }


// JSON* RiakSpooler::ParseJsonConfiguration(DataBuffer& buffer) {
//   // parse the JSON string with the vjson library
//   char *error_pos  = 0;
//   char *error_desc = 0;
//   int error_line   = 0;
//   block_allocator allocator(1 << 10);
//   JSON *root = json_parse((char*)buffer.data,
//                                 &error_pos,
//                                 &error_desc,
//                                 &error_line,
//                                 &allocator);

//   // check if the json string was parsed successfully
//   if (!root) {
//     LogCvmfs(kLogSpooler, kLogStderr, "Failed to parse Riak configuration json "
//                                       "string.\n"
//                                       "Error at line %d: %s\n"
//                                       "%s\n"
//                                       "JSON String was:\n"
//                                       "%s",
//              error_line, error_desc, error_pos, buffer.data);
//     return NULL;
//   }

//   // all fine
//   return root;
// }

// struct CheckResponse { // TODO: C++11: replace this scoped boxing by a typed enum!
//   enum T {
//     Correct,            //!< Parameter was identified and correct
//     NotCorrect,         //!< Parameter was identified but incorrect
//     NotMyResponsibility //!< Parameter was not identified... go on
//   };
// };

// /**
//  * checks a single property JSON object for a certain value. Additionally it is
//  * checked if the property is present, reporting an error if not.
//  *
//  * Note: you need to specialize this template for each data type you like to
//  *       check! Please give a consistent error message, if you notice that some-
//  *       thing is fishy with the configuration.
//  *
//  * @param object         the property JSON object to be checked
//  * @param prop_name      the property name to look for
//  * @param expectation    the expected value for the checked property
//  * @return               Correct                if the right property was found
//  *                                              to be correct
//  *                       NotCorrect             if the right property was found
//  *                                              but it is misconfigured
//  *                       NotMyResponsibility    if we did not hit the right
//  *                                              property
//  */
// template <typename T>
// CheckResponse::T Check(const JSON *object,
//                        const std::string &prop_name,
//                        const T &expectation) {
//   const bool not_implemented = false;
//   assert (not_implemented);
// }

// template <>
// CheckResponse::T Check<bool>(const JSON *object,
//                              const std::string &prop_name,
//                              const bool &expectation) {
//   // check if we got the right object here
//   if (object->type != JSON_BOOL ||
//       object->name != prop_name)
//     return CheckResponse::NotMyResponsibility;

//   // check if the expected value is set
//   if (object->int_value != expectation) {
//     LogCvmfs(kLogSpooler, kLogStderr, "Expected Riak config '%s' to be %s but "
//                                       "turned out to be %s.",
//              prop_name.c_str(), (expectation ? "true" : "false"),
//                                 (object->int_value ? "true" : "false"));
//     return CheckResponse::NotCorrect;
//   }

//   // all good
//   return CheckResponse::Correct;
// }

// template <>
// CheckResponse::T Check<int>(const JSON *object,
//                             const std::string &prop_name,
//                             const int &expectation) {
//   // check if we got the right object here
//   if (object->type != JSON_INT    ||
//       object->name != prop_name)
//     return CheckResponse::NotMyResponsibility;

//   // check if the expected value is set
//   if (object->int_value != expectation) {
//     LogCvmfs(kLogSpooler, kLogStderr, "Expected Riak config '%s' to be %d but "
//                                       "turned out to be %d.",
//              prop_name.c_str(), expectation, object->int_value);
//     return CheckResponse::NotCorrect;
//   }

//   // all good
//   return CheckResponse::Correct;
// }

// /**
//  * goes through all parameters in the JSON configuration and determines if the
//  * given parameter is correctly configured.
//  *
//  * @param object     the JSON configuration object to be checked
//  * @param prop_name  the property name to be identified and checked
//  * @param expection  the asserted setting of the property
//  * @return  true if the property was found AND it was correctly configured
//  */
// template <typename T>
// bool ConfigAssertion(const JSON *object,
//                      const std::string &prop_name,
//                      const T &expectation) {
//   // go through the configuration and check for the right value
//   for (JSON *it = object->first_child; it != NULL; it = it->next_sibling) {
//     CheckResponse::T result = Check<T>(it, prop_name, expectation);
//     if (result == CheckResponse::Correct)    return true;
//     if (result == CheckResponse::NotCorrect) return false;
//   }

//   // element was not found in the configuration... this is fishy!
//   LogCvmfs(kLogSpooler, kLogStderr, "No entry for '%s' found in the given Riak "
//                                     "configuration JSON object.",
//            prop_name.c_str());
//   return false;
// }


// bool RiakSpooler::CheckJsonConfiguration(const JSON *json_root) {
//   // check general structure of JSON configuration
//   if (json_root                    == NULL               ||
//       json_root->type              != JSON_OBJECT        ||
//       json_root->first_child       == NULL               ||
//       json_root->first_child->type != JSON_OBJECT        ||
//       strcmp(json_root->first_child->name, "props") != 0) {
//     LogCvmfs(kLogSpooler, kLogStderr, "Cannot read JSON configuration returned "
//                                       "by Riak.");
//     return false;
//   }

//   // check individual Riak configurations
//   JSON *props = json_root->first_child;
//   return ConfigAssertion(props, "allow_mult",      false) &&
//          ConfigAssertion(props, "last_write_wins", true)  &&
//          ConfigAssertion(props, "n_val",           3);
// }


// // -----------------------------------------------------------------------------


// bool RiakSpooler::DataBuffer::Reserve(const size_t bytes) {
//   unsigned char *new_buffer = (unsigned char*)realloc((void*)data,
//                                                              size_ + bytes);
//   if (new_buffer == NULL)
//     return false;

//   data   = new_buffer;
//   size_ += bytes;
//   return true;
// }


// unsigned char* RiakSpooler::DataBuffer::Position() const {
//   return data + offset_;
// }


// void RiakSpooler::DataBuffer::Copy(const unsigned char* ptr,
//                                       const size_t bytes) {
//   const size_t free_space = size_ - offset_;
//   assert (free_space >= bytes);

//   memcpy(Position(), ptr, bytes);
//   offset_ += bytes;
// }
