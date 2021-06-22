/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_S3FANOUT_H_
#define CVMFS_S3FANOUT_H_

#include <poll.h>
#include <semaphore.h>

#include <climits>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "dns.h"
#include "duplex_curl.h"
#include "prng.h"
#include "util/file_backed_buffer.h"
#include "util/mmap_file.h"
#include "util/pointer.h"
#include "util_concurrency.h"

namespace s3fanout {

enum AuthzMethods {
  kAuthzAwsV2 = 0,
  kAuthzAwsV4,
  kAuthzAzure
};

/**
 * Possible return values.
 */
enum Failures {
  kFailOk = 0,
  kFailLocalIO,
  kFailBadRequest,
  kFailForbidden,
  kFailHostResolve,
  kFailHostConnection,
  kFailNotFound,
  kFailServiceUnavailable,
  kFailRetry,
  kFailOther,

  kFailNumEntries
};  // Failures


inline const char *Code2Ascii(const Failures error) {
  const char *texts[kFailNumEntries + 1];
  texts[0] = "S3: OK";
  texts[1] = "S3: local I/O failure";
  texts[2] = "S3: malformed URL (bad request)";
  texts[3] = "S3: forbidden";
  texts[4] = "S3: failed to resolve host address";
  texts[5] = "S3: host connection problem";
  texts[6] = "S3: not found";
  texts[7] = "S3: service not available";
  texts[8] = "S3: unknown service error, perhaps wrong authentication protocol";
  texts[8] = "S3: too many requests, service asks for backoff and retry";
  texts[9] = "no text";
  return texts[error];
}



struct Statistics {
  double transferred_bytes;
  double transfer_time;
  uint64_t num_requests;
  uint64_t num_retries;
  uint64_t ms_throttled;  // Total waiting time imposed by HTTP 429 replies

  Statistics() {
    transferred_bytes = 0.0;
    transfer_time = 0.0;
    num_requests = 0;
    num_retries = 0;
    ms_throttled = 0;
  }

  std::string Print() const;
};  // Statistics


/**
 * Contains all the information to specify an upload job.
 */
struct JobInfo {
  enum RequestType {
    kReqHeadOnly = 0,  // peek
    kReqHeadPut,  // conditional upload of content-addressed objects
    kReqPutCas,  // immutable data object
    kReqPutDotCvmfs,  // one of the /.cvmfs... top level files
    kReqPutHtml,  // HTML file - display instead of downloading
    kReqPutBucket,  // bucket creation
    kReqDelete,
  };

  const std::string object_key;
  void *callback;  // Callback to be called when job is finished
  UniquePtr<FileBackedBuffer> origin;

  // One constructor per destination
  JobInfo(
    const std::string &object_key,
    void *callback,
    FileBackedBuffer *origin)
    : object_key(object_key),
      origin(origin)
  {
    JobInfoInit();
    this->callback = callback;
  }
  void JobInfoInit() {
    curl_handle = NULL;
    http_headers = NULL;
    callback = NULL;
    request = kReqPutCas;
    error_code = kFailOk;
    http_error = 0;
    num_retries = 0;
    backoff_ms = 0;
    throttle_ms = 0;
    throttle_timestamp = 0;
  }
  ~JobInfo() {}

  // Internal state, don't touch
  CURL *curl_handle;
  struct curl_slist *http_headers;
  uint64_t payload_size;
  RequestType request;
  Failures error_code;
  int http_error;
  unsigned char num_retries;
  // Exponential backoff with cutoff in case of errors
  unsigned backoff_ms;
  // Throttle imposed by HTTP 429 reply; mutually exclusive with backoff_ms
  unsigned throttle_ms;
  // Remember when the 429 reply came in to only throttle if still necessary
  uint64_t throttle_timestamp;
};  // JobInfo

struct S3FanOutDnsEntry {
  S3FanOutDnsEntry() : counter(0), dns_name(), ip(), port("80"),
     clist(NULL), sharehandle(NULL) {}
  unsigned int counter;
  std::string dns_name;
  std::string ip;
  std::string port;
  struct curl_slist *clist;
  CURLSH *sharehandle;
};  // S3FanOutDnsEntry


class S3FanoutManager : SingleCopy {
 protected:
  typedef SynchronizingCounter<uint32_t> Semaphore;

 public:
  // 250ms pause after HTTP 429 "Too Many Retries"
  static const unsigned kDefault429ThrottleMs;
  // Don't throttle for more than a few seconds
  static const unsigned kMax429ThrottleMs;
  // Report throttle operations only every so often
  static const unsigned kThrottleReportIntervalSec;
  static const unsigned kDefaultHTTPPort;

  struct S3Config {
    S3Config() {
      authz_method = kAuthzAwsV2;
      dns_buckets = true;
      pool_max_handles = 0;
      opt_timeout_sec = 20;
      opt_max_retries = 3;
      opt_backoff_init_ms = 100;
      opt_backoff_max_ms = 2000;
    }
    std::string access_key;
    std::string secret_key;
    std::string hostname_port;
    AuthzMethods authz_method;
    std::string region;
    std::string flavor;
    std::string bucket;
    bool dns_buckets;
    uint32_t pool_max_handles;
    unsigned opt_timeout_sec;
    unsigned opt_max_retries;
    unsigned opt_backoff_init_ms;
    unsigned opt_backoff_max_ms;
  };

  static void DetectThrottleIndicator(const std::string &header, JobInfo *info);

  explicit S3FanoutManager(const S3Config &config);

  ~S3FanoutManager();

  void Spawn();

  void PushNewJob(JobInfo *info);
  void PushCompletedJob(JobInfo *info);
  JobInfo *PopCompletedJob();

  const Statistics &GetStatistics();

 private:
  // Reflects the default Apache configuration of the local backend
  static const char *kCacheControlCas;  // Cache-Control: max-age=259200
  static const char *kCacheControlDotCvmfs;  // Cache-Control: max-age=61
  static const unsigned kLowSpeedLimit = 1024;  // Require at least 1kB/s

  static int CallbackCurlSocket(CURL *easy, curl_socket_t s, int action,
                                void *userp, void *socketp);
  static void *MainUpload(void *data);
  std::vector<s3fanout::JobInfo*> jobs_todo_;
  pthread_mutex_t *jobs_todo_lock_;
  pthread_mutex_t *curl_handle_lock_;

  CURL *AcquireCurlHandle() const;
  void ReleaseCurlHandle(JobInfo *info, CURL *handle) const;
  void InitPipeWatchFds();
  int InitializeDnsSettings(CURL *handle,
                            std::string remote_host) const;
  void InitializeDnsSettingsCurl(CURL *handle, CURLSH *sharehandle,
                                 curl_slist *clist) const;
  Failures InitializeRequest(JobInfo *info, CURL *handle) const;
  void SetUrlOptions(JobInfo *info) const;
  void UpdateStatistics(CURL *handle);
  bool CanRetry(const JobInfo *info);
  void Backoff(JobInfo *info);
  bool VerifyAndFinalize(const int curl_error, JobInfo *info);
  std::string GetRequestString(const JobInfo &info) const;
  std::string GetContentType(const JobInfo &info) const;
  std::string GetUriEncode(const std::string &val, bool encode_slash) const;
  std::string GetAwsV4SigningKey(const std::string &date) const;
  bool MkPayloadHash(const JobInfo &info, std::string *hex_hash) const;
  bool MkV2Authz(const JobInfo &info,
                 std::vector<std::string> *headers) const;
  bool MkV4Authz(const JobInfo &info,
                 std::vector<std::string> *headers) const;
  bool MkAzureAuthz(const JobInfo &info,
                 std::vector<std::string> *headers) const;
  std::string MkUrl(const std::string &objkey) const {
    if (config_.dns_buckets) {
      return "http://" + complete_hostname_ + "/" + objkey;
    } else {
      return "http://" + complete_hostname_ + "/" + config_.bucket +
             "/" + objkey;
    }
  }
  std::string MkCompleteHostname() {
    if (config_.dns_buckets) {
      return config_.bucket + "." + config_.hostname_port;
    } else {
      return config_.hostname_port;
    }
  }

  const S3Config config_;
  std::string complete_hostname_;

  Prng prng_;
  /**
   * This is not strictly necessary but it helps the debugging
   */
  std::set<JobInfo *> *active_requests_;

  std::set<CURL *> *pool_handles_idle_;
  std::set<CURL *> *pool_handles_inuse_;
  std::set<S3FanOutDnsEntry *> *sharehandles_;
  std::map<CURL *, S3FanOutDnsEntry *> *curl_sharehandles_;
  dns::CaresResolver *resolver_;
  CURLM *curl_multi_;
  std::string *user_agent_;

  /**
   * AWS4 signing keys are derived from the secret key, a region and a date.
   * The signing key for current day can be cached.
   */
  mutable std::pair<std::string, std::string> last_signing_key_;

  pthread_t thread_upload_;
  atomic_int32 multi_threaded_;

  struct pollfd *watch_fds_;
  uint32_t watch_fds_size_;
  uint32_t watch_fds_inuse_;
  uint32_t watch_fds_max_;

  // A pipe used to signal termination from S3FanoutManager to MainUpload
  // thread. Anything written into it results in MainUpload thread exit.
  int pipe_terminate_[2];
  // A pipe to used to push jobs from S3FanoutManager to MainUpload thread.
  // S3FanoutManager writes a JobInfo* pointer. MainUpload then reads the
  // pointer and processes the job.
  int pipe_jobs_[2];
  // A pipe used to collect completed jobs.  MainUpload writes in the
  // pointer to the completed job.  PopCompletedJob() used to
  // retrieve pointer.
  int pipe_completed_[2];

  bool opt_ipv4_only_;

  unsigned int max_available_jobs_;
  Semaphore *available_jobs_;

  // Writes and reads should be atomic because reading happens in a different
  // thread than writing.
  Statistics *statistics_;

  // Report not every occurance of throtteling but only every so often
  uint64_t timestamp_last_throttle_report_;

  bool is_curl_debug_;
};  // S3FanoutManager

}  // namespace s3fanout

#endif  // CVMFS_S3FANOUT_H_
