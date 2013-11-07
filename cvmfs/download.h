/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_DOWNLOAD_H_
#define CVMFS_DOWNLOAD_H_

#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include <poll.h>

#include <cstdio>

#include <string>
#include <vector>
#include <set>

#include "duplex_curl.h"
#include "compression.h"
#include "prng.h"
#include "hash.h"
#include "atomic.h"

namespace download {

/**
 * Where to store downloaded data.
 */
enum Destination {
  kDestinationMem = 1,
  kDestinationFile,
  kDestinationPath,
  kDestinationNone
};  // Destination

/**
 * Possible return values.
 */
enum Failures {
  kFailOk = 0,
  kFailLocalIO,
  kFailBadUrl,
  kFailProxyResolve,
  kFailHostResolve,
  // artificial failure code.  Try other host even though
  // failure seems to be at the proxy
  kFailHostAfterProxy,
  kFailProxyConnection,
  kFailHostConnection,
  kFailProxyHttp,
  kFailHostHttp,
  kFailBadData,
  kFailOther,
};  // Failures


struct Statistics {
  double transferred_bytes;
  double transfer_time;
  uint64_t num_requests;
  uint64_t num_retries;
  uint64_t num_proxy_failover;
  uint64_t num_host_failover;

  Statistics() {
    transferred_bytes = 0.0;
    transfer_time = 0.0;
    num_requests = 0;
    num_retries = 0;
    num_proxy_failover = 0;
    num_host_failover = 0;
  }

  std::string Print() const;
};  // Statistics


/**
 * Contains all the information to specify a download job.
 */
struct JobInfo {
  const std::string *url;
  bool compressed;
  bool probe_hosts;
  bool head_request;
  Destination destination;
  struct {
    size_t size;
    size_t pos;
    char *data;
  } destination_mem;
  FILE *destination_file;
  const std::string *destination_path;
  const shash::Any *expected_hash;

  // One constructor per destination + head request
  JobInfo() { wait_at[0] = wait_at[1] = -1; head_request = false; }
  JobInfo(const std::string *u, const bool c, const bool ph,
          const std::string *p, const shash::Any *h) : url(u), compressed(c),
          probe_hosts(ph), head_request(false),
          destination(kDestinationPath), destination_path(p), expected_hash(h)
          { wait_at[0] = wait_at[1] = -1; }
  JobInfo(const std::string *u, const bool c, const bool ph, FILE *f,
          const shash::Any *h) : url(u), compressed(c), probe_hosts(ph),
          head_request(false),
          destination(kDestinationFile), destination_file(f), expected_hash(h)
          { wait_at[0] = wait_at[1] = -1; }
  JobInfo(const std::string *u, const bool c, const bool ph,
          const shash::Any *h) : url(u), compressed(c), probe_hosts(ph),
          head_request(false), destination(kDestinationMem), expected_hash(h)
          { wait_at[0] = wait_at[1] = -1; }
  JobInfo(const std::string *u, const bool ph) :
          url(u), compressed(false), probe_hosts(ph), head_request(true),
          destination(kDestinationNone), expected_hash(NULL)
          { wait_at[0] = wait_at[1] = -1; }
  ~JobInfo() {
    if (wait_at[0] >= 0) {
      close(wait_at[0]);
      close(wait_at[1]);
    }
  }

  // Internal state, don't touch
  CURL *curl_handle;
  z_stream zstream;
  shash::ContextPtr hash_context;
  int wait_at[2];  /**< Pipe used for the return value */
  std::string proxy;
  bool nocache;
  Failures error_code;
  unsigned char num_used_proxies;
  unsigned char num_used_hosts;
  unsigned char num_retries;
  unsigned backoff_ms;
};  // JobInfo


class DownloadManager {
 public:
  DownloadManager();
  ~DownloadManager();

  void Init(const unsigned max_pool_handles, const bool use_system_proxy);
  void Fini();
  void Spawn();
  Failures Fetch(JobInfo *info);

  void SetDnsServer(const std::string &address);
  void SetTimeout(const unsigned seconds_proxy, const unsigned seconds_direct);
  void GetTimeout(unsigned *seconds_proxy, unsigned *seconds_direct);
  const Statistics &GetStatistics();
  void SetHostChain(const std::string &host_list);
  void GetHostInfo(std::vector<std::string> *host_chain,
                   std::vector<int> *rtt, unsigned *current_host);
  void ProbeHosts();
  void SwitchHost();
  void SetProxyChain(const std::string &proxy_list);
  void GetProxyInfo(std::vector< std::vector<std::string> > *proxy_chain,
                    unsigned *current_group);
  void RebalanceProxies();
  void SwitchProxyGroup();
  void SetProxyGroupResetDelay(const unsigned seconds);
  void GetProxyBackupInfo(unsigned *reset_delay, time_t *timestamp_failover);
  void SetHostResetDelay(const unsigned seconds);
  void GetHostBackupInfo(unsigned *reset_delay, time_t *timestamp_failover);
  void SetRetryParameters(const unsigned max_retries,
                          const unsigned backoff_init_ms,
                          const unsigned backoff_max_ms);
  void ActivatePipelining();
 private:
  static int CallbackCurlSocket(CURL *easy, curl_socket_t s, int action,
                                void *userp, void *socketp);
  static void *MainDownload(void *data);

  void SwitchHost(JobInfo *info);
  void SwitchProxy(JobInfo *info);
  void RebalanceProxiesUnlocked();
  CURL *AcquireCurlHandle();
  void ReleaseCurlHandle(CURL *handle);
  void InitializeRequest(JobInfo *info, CURL *handle);
  void SetUrlOptions(JobInfo *info);
  void UpdateStatistics(CURL *handle);
  bool CanRetry(const JobInfo *info);
  void Backoff(JobInfo *info);
  bool VerifyAndFinalize(const int curl_error, JobInfo *info);

  Prng prng_;
  std::set<CURL *> *pool_handles_idle_;
  std::set<CURL *> *pool_handles_inuse_;
  uint32_t pool_max_handles_;
  CURLM *curl_multi_;
  curl_slist *http_headers_;
  curl_slist *http_headers_nocache_;

  pthread_t thread_download_;
  atomic_int32 multi_threaded_;
  int pipe_terminate_[2];

  int pipe_jobs_[2];
  struct pollfd *watch_fds_;
  uint32_t watch_fds_size_;
  uint32_t watch_fds_inuse_;
  uint32_t watch_fds_max_;

  pthread_mutex_t *lock_options_;
  pthread_mutex_t *lock_synchronous_mode_;
  char *opt_dns_server_;
  unsigned opt_timeout_proxy_ ;
  unsigned opt_timeout_direct_;
  std::vector<std::string> *opt_host_chain_;
  std::vector<int> *opt_host_chain_rtt_; /**< created by SetHostChain(),
                                            filled by probe_hosts.  Contains time to get .cvmfschecksum in ms.
                                            -1 is unprobed, -2 is error */
  unsigned opt_host_chain_current_;
  std::vector< std::vector<std::string> > *opt_proxy_groups_;
  unsigned opt_proxy_groups_current_;
  unsigned opt_proxy_groups_current_burned_;
  unsigned opt_num_proxies_;

  unsigned opt_max_retries_;
  unsigned opt_backoff_init_ms_;
  unsigned opt_backoff_max_ms_;

  bool opt_ipv4_only_;

  /**
   * More than one proxy group can be considered as group of primary proxies
   * followed by backup proxy groups, e.g. at another site.
   * If opt_proxy_groups_reset_after_ is > 0, cvmfs will reset its proxy group
   * to the first one after opt_proxy_groups_reset_after_ seconds are elapsed.
   */
  time_t opt_timestamp_backup_proxies_;
  time_t opt_timestamp_failover_proxies_;  // failover within the same group
  unsigned opt_proxy_groups_reset_after_;

  /**
   * Similarly to proxy group reset, we'd also like to reset the host after a
   * failover.  Host outages can last longer and might come with a separate
   * reset delay.
   */
  time_t opt_timestamp_backup_host_;
  unsigned opt_host_reset_after_;

  // Writes and reads should be atomic because reading happens in a different
  // thread than writing.
  Statistics *statistics_;
};  // DownloadManager

}  // namespace download

#endif  // CVMFS_DOWNLOAD_H_
