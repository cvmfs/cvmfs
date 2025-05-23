/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NETWORK_JOBINFO_H_
#define CVMFS_NETWORK_JOBINFO_H_

#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <unistd.h>

#include <cstdio>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "compression/compression.h"
#include "crypto/hash.h"
#include "duplex_curl.h"
#include "network/network_errors.h"
#include "network/sink.h"
#include "network/sink_file.h"
#include "network/sink_mem.h"
#include "network/sink_path.h"
#include "util/pipe.h"
#include "util/tube.h"

class InterruptCue;

namespace download {

enum DataTubeAction {
  kActionStop = 0,
  kActionContinue,
  kActionDecompress
};

/**
 * Wrapper for the data tube to transfer data from CallbackCurlData() that is
 * executed in MainDownload() Thread to Fetch() called by a fuse thread
 *
 * TODO(heretherebedragons): do we want to have a pool of those
 * datatubeelements?
 */
struct DataTubeElement : SingleCopy {
  char *data;
  size_t size;
  DataTubeAction action;

  explicit DataTubeElement(DataTubeAction xact)
      : data(NULL), size(0), action(xact) { }
  DataTubeElement(char *mov_data, size_t xsize, DataTubeAction xact)
      : data(mov_data), size(xsize), action(xact) { }

  ~DataTubeElement() { delete data; }
};

/**
 * Contains all the information to specify a download job.
 */
class JobInfo {
 private:
  static atomic_int64 next_uuid;
  int64_t id_;
  /// Pipe used for the return value
  UniquePtr<Pipe<kPipeDownloadJobsResults> > pipe_job_results;
  /// Tube (bounded thread-safe queue) to transport data from CURL callback
  /// to be decompressed in Fetch() instead of MainDownload()
  UniquePtr<Tube<DataTubeElement> > data_tube_;
  const std::string *url_;
  bool compressed_;
  bool probe_hosts_;
  bool head_request_;
  bool follow_redirects_;
  bool force_nocache_;
  pid_t pid_;
  uid_t uid_;
  gid_t gid_;
  void *cred_data_;  // Per-transfer credential data
  InterruptCue *interrupt_cue_;
  cvmfs::Sink *sink_;
  const shash::Any *expected_hash_;
  const std::string *extra_info_;

  // Allow byte ranges to be specified.
  off_t range_offset_;
  off_t range_size_;

  // Internal state
  CURL *curl_handle_;
  curl_slist *headers_;
  char *info_header_;
  char *tracing_header_pid_;
  char *tracing_header_gid_;
  char *tracing_header_uid_;
  z_stream zstream_;
  shash::ContextPtr hash_context_;
  std::string proxy_;
  std::string link_;
  bool nocache_;
  Failures error_code_;
  int http_code_;
  unsigned char num_used_proxies_;
  unsigned char num_used_metalinks_;
  unsigned char num_used_hosts_;
  unsigned char num_retries_;
  unsigned backoff_ms_;
  int current_metalink_chain_index_;
  int current_host_chain_index_;

  // Don't fail-over proxies on download errors. default = false
  bool allow_failure_;

  // TODO(heretherebedragons) c++11 allows to delegate constructors (N1986)
  // Replace Init() with JobInfo() that is called by the other constructors
  void Init();

 public:
  /**
   * Sink version: downloads entire data chunk where URL u points to
   */
  JobInfo(const std::string *u, const bool c, const bool ph,
          const shash::Any *h, cvmfs::Sink *s);

  /**
   * No sink version: Only downloads header where the URL u points to
   */
  JobInfo(const std::string *u, const bool ph);

  ~JobInfo() {
    pipe_job_results.Destroy();
    data_tube_.Destroy();
  }

  void CreatePipeJobResults() {
    pipe_job_results = new Pipe<kPipeDownloadJobsResults>();
  }

  bool IsValidPipeJobResults() { return pipe_job_results.IsValid(); }

  void CreateDataTube() {
    // TODO(heretherebedragons) change to weighted queue
    data_tube_ = new Tube<DataTubeElement>(500);
  }

  bool IsValidDataTube() { return data_tube_.IsValid(); }

  /**
   * Tells whether the error is because of a non-existing file. Should only
   * be called if error_code is not kFailOk
   */
  bool IsFileNotFound();

  pid_t *GetPidPtr() { return &pid_; }
  uid_t *GetUidPtr() { return &uid_; }
  gid_t *GetGidPtr() { return &gid_; }
  InterruptCue **GetInterruptCuePtr() { return &interrupt_cue_; }
  z_stream *GetZstreamPtr() { return &zstream_; }
  Failures *GetErrorCodePtr() { return &error_code_; }
  void **GetCredDataPtr() { return &cred_data_; }
  curl_slist **GetHeadersPtr() { return &headers_; }
  CURL **GetCurlHandle() { return &curl_handle_; }
  shash::ContextPtr *GetHashContextPtr() { return &hash_context_; }
  Pipe<kPipeDownloadJobsResults> *GetPipeJobResultPtr() {
    return pipe_job_results.weak_ref();
  }
  Tube<DataTubeElement> *GetDataTubePtr() { return data_tube_.weak_ref(); }

  const std::string *url() const { return url_; }
  bool compressed() const { return compressed_; }
  bool probe_hosts() const { return probe_hosts_; }
  bool head_request() const { return head_request_; }
  bool follow_redirects() const { return follow_redirects_; }
  bool force_nocache() const { return force_nocache_; }
  pid_t pid() const { return pid_; }
  uid_t uid() const { return uid_; }
  gid_t gid() const { return gid_; }
  void *cred_data() const { return cred_data_; }
  InterruptCue *interrupt_cue() const { return interrupt_cue_; }
  cvmfs::Sink *sink() const { return sink_; }
  const shash::Any *expected_hash() const { return expected_hash_; }
  const std::string *extra_info() const { return extra_info_; }

  off_t range_offset() const { return range_offset_; }
  off_t range_size() const { return range_size_; }

  CURL *curl_handle() const { return curl_handle_; }
  curl_slist *headers() const { return headers_; }
  char *info_header() const { return info_header_; }
  char *tracing_header_pid() const { return tracing_header_pid_; }
  char *tracing_header_gid() const { return tracing_header_gid_; }
  char *tracing_header_uid() const { return tracing_header_uid_; }
  z_stream zstream() const { return zstream_; }
  shash::ContextPtr hash_context() const { return hash_context_; }
  std::string proxy() const { return proxy_; }
  std::string link() const { return link_; }
  bool nocache() const { return nocache_; }
  Failures error_code() const { return error_code_; }
  int http_code() const { return http_code_; }
  unsigned char num_used_proxies() const { return num_used_proxies_; }
  unsigned char num_used_metalinks() const { return num_used_metalinks_; }
  unsigned char num_used_hosts() const { return num_used_hosts_; }
  unsigned char num_retries() const { return num_retries_; }
  unsigned backoff_ms() const { return backoff_ms_; }
  int current_metalink_chain_index() const {
    return current_metalink_chain_index_;
  }
  int current_host_chain_index() const { return current_host_chain_index_; }

  bool allow_failure() const { return allow_failure_; }
  int64_t id() const { return id_; }


  void SetUrl(const std::string *url) { url_ = url; }
  void SetCompressed(bool compressed) { compressed_ = compressed; }
  void SetProbeHosts(bool probe_hosts) { probe_hosts_ = probe_hosts; }
  void SetHeadRequest(bool head_request) { head_request_ = head_request; }
  void SetFollowRedirects(bool follow_redirects) {
    follow_redirects_ = follow_redirects;
  }
  void SetForceNocache(bool force_nocache) { force_nocache_ = force_nocache; }
  void SetPid(pid_t pid) { pid_ = pid; }
  void SetUid(uid_t uid) { uid_ = uid; }
  void SetGid(gid_t gid) { gid_ = gid; }
  void SetCredData(void *cred_data) { cred_data_ = cred_data; }
  void SetInterruptCue(InterruptCue *interrupt_cue) {
    interrupt_cue_ = interrupt_cue;
  }
  void SetSink(cvmfs::Sink *sink) { sink_ = sink; }
  void SetExpectedHash(const shash::Any *expected_hash) {
    expected_hash_ = expected_hash;
  }
  void SetExtraInfo(const std::string *extra_info) { extra_info_ = extra_info; }

  void SetRangeOffset(off_t range_offset) { range_offset_ = range_offset; }
  void SetRangeSize(off_t range_size) { range_size_ = range_size; }

  void SetCurlHandle(CURL *curl_handle) { curl_handle_ = curl_handle; }
  void SetHeaders(curl_slist *headers) { headers_ = headers; }
  void SetInfoHeader(char *info_header) { info_header_ = info_header; }
  void SetTracingHeaderPid(char *tracing_header_pid) {
    tracing_header_pid_ = tracing_header_pid;
  };
  void SetTracingHeaderGid(char *tracing_header_gid) {
    tracing_header_gid_ = tracing_header_gid;
  };
  void SetTracingHeaderUid(char *tracing_header_uid) {
    tracing_header_uid_ = tracing_header_uid;
  };
  void SetZstream(z_stream zstream) { zstream_ = zstream; }
  void SetHashContext(shash::ContextPtr hash_context) {
    hash_context_ = hash_context;
  }
  void SetProxy(const std::string &proxy) { proxy_ = proxy; }
  void SetLink(const std::string &link) { link_ = link; }
  void SetNocache(bool nocache) { nocache_ = nocache; }
  void SetErrorCode(Failures error_code) { error_code_ = error_code; }
  void SetHttpCode(int http_code) { http_code_ = http_code; }
  void SetNumUsedProxies(unsigned char num_used_proxies) {
    num_used_proxies_ = num_used_proxies;
  }
  void SetNumUsedMetalinks(unsigned char num_used_metalinks) {
    num_used_metalinks_ = num_used_metalinks;
  }
  void SetNumUsedHosts(unsigned char num_used_hosts) {
    num_used_hosts_ = num_used_hosts;
  }
  void SetNumRetries(unsigned char num_retries) { num_retries_ = num_retries; }
  void SetBackoffMs(unsigned backoff_ms) { backoff_ms_ = backoff_ms; }
  void SetCurrentMetalinkChainIndex(int current_metalink_chain_index) {
    current_metalink_chain_index_ = current_metalink_chain_index;
  }
  void SetCurrentHostChainIndex(int current_host_chain_index) {
    current_host_chain_index_ = current_host_chain_index;
  }

  void SetAllowFailure(bool allow_failure) { allow_failure_ = allow_failure; }

  // needed for fetch.h ThreadLocalStorage
  JobInfo() { Init(); }
};  // JobInfo

}  // namespace download

#endif  // CVMFS_NETWORK_JOBINFO_H_
