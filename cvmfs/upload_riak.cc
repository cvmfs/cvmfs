#include "upload_riak.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>

#include "duplex_curl.h"
#include "logging.h"

using namespace upload;

RiakPushWorker::Context* RiakPushWorker::GenerateContext(
                            SpoolerBackend<RiakPushWorker> *master,
                            const std::string              &upstream_urls) {
  std::vector<std::string> upstream_url_vector = SplitString(upstream_urls, '@');
  return new Context(master, upstream_url_vector);
}


int RiakPushWorker::GetNumberOfWorkers(const Context *context) {
  return context->upstream_urls.size() * GetNumberOfCpuCores();
}


const std::string& RiakPushWorker::Context::AcquireUpstreamUrl() const {
  assert (upstream_urls.size() > 0);
  // get an unique upstream URL and set the pointer to the next one...
  const std::string& result = upstream_urls[next_upstream_url_];
  next_upstream_url_ = (next_upstream_url_ + 1) % upstream_urls.size();
  return result;
}


RiakPushWorker::RiakPushWorker(Context* context) :
  context_(context),
  initialized_(false),
  http_headers_(NULL)
{
  LockGuard<Context> lock(context);
  upstream_url_ = context->AcquireUpstreamUrl();
}


RiakPushWorker::~RiakPushWorker() {
  curl_easy_cleanup(curl_);
  curl_slist_free_all(http_headers_);
  curl_global_cleanup();
}


bool RiakPushWorker::Initialize() {
  bool retval = AbstractPushWorker::Initialize();
  if (!retval)
    return false;

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


void RiakPushWorker::ProcessCopyJob(StorageCopyJob *copy_job) {
  if (copy_job->move()) {
    LogCvmfs(kLogSpooler, kLogStderr, "RiakPushWorker does not support "
                                      "move at the moment.");
    copy_job->Failed();
    return;
  }

  const int retval = PushFileToRiak(GenerateRiakKey(copy_job->remote_path()),
                                    copy_job->local_path());

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

  const std::string &local_path   = compression_job->local_path();
        hash::Any   &content_hash = compression_job->content_hash();

  // compress the file to a temporary location
  static const std::string tmp_dir = "/ramdisk";
  std::string tmp_file_path;
  hash::Any compressed_hash(hash::kSha1);
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

  // push to Riak
  const int retval = PushFileToRiak(GenerateRiakKey(compression_job),
                                     tmp_file_path);

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


int RiakPushWorker::PushFileToRiak(const std::string &key,
                                   const std::string &file_path) {
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "pushing file %s to Riak using key %s",
           file_path.c_str(), key.c_str());

  const std::string url = CreateRequestUrl(key);
  FILE *hd_src;

  CURLcode res;
  int      retcode = -1;

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

  // set url for Riak put command
  if (curl_easy_setopt(curl_, CURLOPT_URL, url.c_str()) != CURLE_OK) {
    retcode = 3;
    goto out;
  }

  // set file size of the file to be uploaded
  if (curl_easy_setopt(curl_, CURLOPT_INFILESIZE_LARGE,
                       (curl_off_t)file_info.st_size) != CURLE_OK) {
    retcode = 4;
    goto out;
  }

  // specify the file handle to be uploaded
  if (curl_easy_setopt(curl_, CURLOPT_READDATA, hd_src) != CURLE_OK) {
    retcode = 5;
    goto out;
  }

  // do the actual business
  res = curl_easy_perform(curl_);
  if(res != CURLE_OK) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to upload %s to Riak node %s "
                                      "because: '%s'",
             file_path.c_str(), url.c_str(), curl_easy_strerror(res));
    retcode = 6;
    goto out;
  }

  // all went well...
  retcode = 0;

out:
  // close the uploaded file
  fclose(hd_src);
  return retcode;
}



std::string RiakPushWorker::CreateRequestUrl(const std::string &key) const {
  return upstream_url_ + "/" + key;
}


bool RiakPushWorker::IsReady() const {
  const bool ready = AbstractPushWorker::IsReady();
  return ready && initialized_;
}
