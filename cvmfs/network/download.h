/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NETWORK_DOWNLOAD_H_
#define CVMFS_NETWORK_DOWNLOAD_H_

#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <unistd.h>

#include <cstdio>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "gtest/gtest_prod.h"

#include "compression/compression.h"
#include "crypto/hash.h"
#include "duplex_curl.h"
#include "network/dns.h"
#include "network/health_check.h"
#include "network/jobinfo.h"
#include "network/network_errors.h"
#include "network/sharding_policy.h"
#include "network/sink.h"
#include "ssl.h"
#include "statistics.h"
#include "util/atomic.h"
#include "util/pipe.h"
#include "util/pointer.h"
#include "util/prng.h"
#include "util/shared_ptr.h"

class InterruptCue;

namespace download {

struct Counters {
  perf::Counter *sz_transferred_bytes;
  perf::Counter *sz_transfer_time;  // measured in milliseconds
  perf::Counter *n_requests;
  perf::Counter *n_retries;
  perf::Counter *n_metalink_failover;
  perf::Counter *n_host_failover;
  perf::Counter *n_proxy_failover;

  explicit Counters(perf::StatisticsTemplate statistics) {
    sz_transferred_bytes = statistics.RegisterTemplated("sz_transferred_bytes",
        "Number of transferred bytes");
    sz_transfer_time = statistics.RegisterTemplated("sz_transfer_time",
        "Transfer time (milliseconds)");
    n_requests = statistics.RegisterTemplated("n_requests",
        "Number of requests");
    n_retries = statistics.RegisterTemplated("n_retries", "Number of retries");
    n_metalink_failover = statistics.RegisterTemplated("n_metalink_failover",
        "Number of metalink failovers");
    n_host_failover = statistics.RegisterTemplated("n_host_failover",
        "Number of host failovers");
    n_proxy_failover = statistics.RegisterTemplated("n_proxy_failover",
        "Number of proxy failovers");
  }
};  // Counters

/**
 * Manages blocks of arrays of curl_slist storing header strings.  In contrast
 * to curl's slists, these ones don't take ownership of the header strings.
 * Overall number of elements is limited as number of concurrent connections
 * is limited.
 *
 * Only use curl_slist objects created in the same HeaderLists instance in this
 * class
 */
class HeaderLists {
  FRIEND_TEST(T_HeaderLists, Intrinsics);
 public:
  ~HeaderLists();
  curl_slist *GetList(const char *header);
  curl_slist *DuplicateList(curl_slist *slist);
  void AppendHeader(curl_slist *slist, const char *header);
  void CutHeader(const char *header, curl_slist **slist);
  void PutList(curl_slist *slist);
  std::string Print(curl_slist *slist);

 private:
  static const unsigned kBlockSize = 4096/sizeof(curl_slist);

  bool IsUsed(curl_slist *slist) { return slist->data != NULL; }
  curl_slist *Get(const char *header);
  void Put(curl_slist *slist);
  void AddBlock();

  std::vector<curl_slist *> blocks_;  // List of curl_slist blocks
};


/**
 * Provides hooks to attach per-transfer credentials to curl handles.
 * Overwritten by the AuthzX509Attachment in authz_curl.cc.  Needs to be
 * thread-safe because it can be potentially used by multiple DownloadManagers.
 */
class CredentialsAttachment {
 public:
  virtual ~CredentialsAttachment() { }
  virtual bool ConfigureCurlHandle(CURL *curl_handle,
                                   pid_t pid,
                                   void **info_data) = 0;
  virtual void ReleaseCurlHandle(CURL *curl_handle, void *info_data) = 0;
};


/**
 * Note when adding new fields: Clone() probably needs to be adjusted, too.
 * TODO(jblomer): improve ordering of members
 */
class DownloadManager {  // NOLINT(clang-analyzer-optin.performance.Padding)
  FRIEND_TEST(T_Download, ValidateGeoReply);
  FRIEND_TEST(T_Download, StripDirect);
  FRIEND_TEST(T_Download, EscapeUrl);

 public:
  // HostInfo is used for both metalink and host
  struct HostInfo {
    HostInfo() { }
    HostInfo(
        std::vector<std::string> *chain,
        const int current,
        const time_t timestamp_backup,
        const unsigned reset_after)
      : chain(chain)
      , current(current)
      , timestamp_backup(timestamp_backup)
      , reset_after(reset_after)
    { }
    std::vector<std::string> *chain;
    int current;
    time_t timestamp_backup;
    unsigned reset_after;
  };

  struct ProxyInfo {
    ProxyInfo() { }
    explicit ProxyInfo(const std::string &url) : url(url) { }
    ProxyInfo(const dns::Host &host, const std::string &url)
      : host(host)
      , url(url)
    { }
    std::string Print();
    dns::Host host;
    std::string url;
  };

  enum ProxySetModes {
    kSetProxyRegular = 0,
    kSetProxyFallback,
    kSetProxyBoth,
  };

  /**
   * No attempt was made to order stratum 1 servers
   */
  static const int kProbeUnprobed;
  /**
   * The rtt to a stratum 1 could not be determined because the stratum 1
   * was unreachable.
   */
  static const int kProbeDown;
  /**
   * The stratum 1 server was put in order according to a Geo-API result
   */
  static const int kProbeGeo;

  static const unsigned kDnsDefaultRetries = 1;
  static const unsigned kDnsDefaultTimeoutMs = 3000;
  static const unsigned kProxyMapScale = 16;

  DownloadManager(const unsigned max_pool_handles,
                  const perf::StatisticsTemplate &statistics,
                  const std::string &name = "standard");
  ~DownloadManager();

  static int ParseHttpCode(const char digits[3]);

  void Spawn();
  DownloadManager *Clone(const perf::StatisticsTemplate &statistics,
                         const std::string &cloned_name);
  Failures Fetch(JobInfo *info);

  void SetCredentialsAttachment(CredentialsAttachment *ca);
  std::string GetDnsServer() const;
  void SetDnsServer(const std::string &address);
  void SetDnsParameters(const unsigned retries, const unsigned timeout_ms);
  void SetDnsTtlLimits(const unsigned min_seconds, const unsigned max_seconds);
  void SetIpPreference(const dns::IpPreference preference);
  void SetTimeout(const unsigned seconds_proxy, const unsigned seconds_direct);
  void GetTimeout(unsigned *seconds_proxy, unsigned *seconds_direct);
  void SetLowSpeedLimit(const unsigned low_speed_limit);
  void SetMetalinkChain(const std::string &metalink_list);
  void SetMetalinkChain(const std::vector<std::string> &metalink_list);
  void GetMetalinkInfo(std::vector<std::string> *metalink_chain,
                       unsigned *current_metalink);
  void SwitchMetalink();
  bool CheckMetalinkChain(const time_t now);
  void SetHostChain(const std::string &host_list);
  void SetHostChain(const std::vector<std::string> &host_list);
  void GetHostInfo(std::vector<std::string> *host_chain,
                   std::vector<int> *rtt, unsigned *current_host);
  void ProbeHosts();
  bool ProbeGeo();
    // Sort list of servers using the Geo API.  If the output_order
    // vector is NULL, then the servers vector input is itself sorted.
    // If it is non-NULL, then servers is left unchanged and the zero-based
    // ordering is stored into output_order.
  bool GeoSortServers(std::vector<std::string> *servers,
                      std::vector<uint64_t>    *output_order = NULL);
  void SwitchHost();
  void SetProxyChain(const std::string &proxy_list,
                     const std::string &fallback_proxy_list,
                     const ProxySetModes set_mode);
  void GetProxyInfo(std::vector< std::vector<ProxyInfo> > *proxy_chain,
                    unsigned *current_group,
                    unsigned *fallback_group);
  std::string GetProxyList();
  std::string GetFallbackProxyList();
  void ShardProxies();
  void RebalanceProxies();
  void SwitchProxyGroup();
  void SetProxyGroupResetDelay(const unsigned seconds);
  void SetMetalinkResetDelay(const unsigned seconds);
  void SetHostResetDelay(const unsigned seconds);
  void SetRetryParameters(const unsigned max_retries,
                          const unsigned backoff_init_ms,
                          const unsigned backoff_max_ms);
  void SetMaxIpaddrPerProxy(unsigned limit);
  void SetProxyTemplates(const std::string &direct, const std::string &forced);
  void EnableInfoHeader();
  void EnableRedirects();
  void EnableIgnoreSignatureFailures();
  void EnableHTTPTracing();
  void AddHTTPTracingHeader(const std::string &header);
  void UseSystemCertificatePath();

  bool SetShardingPolicy(const ShardingPolicySelector type);
  void SetFailoverIndefinitely();
  void SetFqrn(const std::string &fqrn) { fqrn_ = fqrn; }

  unsigned num_hosts() {
    if (opt_host_.chain) return opt_host_.chain->size();
    return 0;
  }

  unsigned num_metalinks() {
    if (opt_metalink_.chain) return opt_metalink_.chain->size();
    return 0;
  }

  dns::IpPreference opt_ip_preference() const {
    return opt_ip_preference_;
  }

 private:
  static int CallbackCurlSocket(CURL *easy, curl_socket_t s, int action,
                                void *userp, void *socketp);
  static void *MainDownload(void *data);

  bool StripDirect(const std::string &proxy_list, std::string *cleaned_list);
  bool ValidateGeoReply(const std::string &reply_order,
                        const unsigned expected_size,
                        std::vector<uint64_t> *reply_vals);
  void SwitchHostInfo(const std::string &typ, HostInfo &info, JobInfo *jobinfo);
  void SwitchMetalink(JobInfo *info);
  void SwitchHost(JobInfo *info);
  void SwitchProxy(JobInfo *info);
  ProxyInfo *ChooseProxyUnlocked(const shash::Any *hash);
  void UpdateProxiesUnlocked(const std::string &reason);
  void RebalanceProxiesUnlocked(const std::string &reason);
  CURL *AcquireCurlHandle();
  void ReleaseCurlHandle(CURL *handle);
  void ReleaseCredential(JobInfo *info);
  void InitializeRequest(JobInfo *info, CURL *handle);
  void SetUrlOptions(JobInfo *info);
  bool ValidateProxyIpsUnlocked(const std::string &url, const dns::Host &host);
  void UpdateStatistics(CURL *handle);
  bool CanRetry(const JobInfo *info);
  void Backoff(JobInfo *info);
  void SetNocache(JobInfo *info);
  void SetRegularCache(JobInfo *info);
  void ProcessLink(JobInfo *info);
  bool VerifyAndFinalize(const int curl_error, JobInfo *info);
  void InitHeaders();
  void CloneProxyConfig(DownloadManager *clone);
  void CheckHostInfoReset(const std::string &typ, HostInfo &info,
                          JobInfo *jobinfo, time_t &now);

  bool EscapeUrlChar(unsigned char input, char output[3]);
  std::string EscapeUrl(const int64_t jobinfo_id, const std::string &url);
  unsigned EscapeHeader(const std::string &header, char *escaped_buf,
                        size_t buf_size);

  inline std::vector<ProxyInfo> *current_proxy_group() const {
    return (opt_proxy_groups_ ?
            &((*opt_proxy_groups_)[opt_proxy_groups_current_]) : NULL);
  }

  Prng prng_;
  std::set<CURL *> *pool_handles_idle_;
  std::set<CURL *> *pool_handles_inuse_;
  uint32_t pool_max_handles_;
  CURLM *curl_multi_;
  HeaderLists *header_lists_;
  curl_slist *default_headers_;
  char *user_agent_;

  pthread_t thread_download_;
  atomic_int32 multi_threaded_;
  UniquePtr<Pipe<kPipeThreadTerminator> > pipe_terminate_;

  UniquePtr<Pipe<kPipeDownloadJobs> > pipe_jobs_;
  struct pollfd *watch_fds_;
  uint32_t watch_fds_size_;
  uint32_t watch_fds_inuse_;
  uint32_t watch_fds_max_;

  pthread_mutex_t *lock_options_;
  pthread_mutex_t *lock_synchronous_mode_;
  std::string opt_dns_server_;
  unsigned opt_timeout_proxy_;
  unsigned opt_timeout_direct_;
  unsigned opt_low_speed_limit_;
  unsigned opt_max_retries_;
  unsigned opt_backoff_init_ms_;
  unsigned opt_backoff_max_ms_;
  bool enable_info_header_;
  bool opt_ipv4_only_;
  bool follow_redirects_;

  /**
   * Ignore signature failures during download.
   * In general it is a bad idea to do this!
   */
  bool ignore_signature_failures_;

  bool enable_http_tracing_;
  std::vector<std::string> http_tracing_headers_;

  // Metalink list
  HostInfo opt_metalink_;
  time_t opt_metalink_timestamp_link_;

  // Host list
  HostInfo opt_host_;
  /**
   * Created by SetHostChain(), filled by probe_hosts.  Contains time to get
   * .cvmfschecksum in ms. -1 is unprobed, -2 is error.
   */
  std::vector<int> *opt_host_chain_rtt_;

  // Proxy list
  std::vector< std::vector<ProxyInfo> > *opt_proxy_groups_;
  /**
   * The current load-balancing group (first dimension in opt_proxy_groups_).
   */
  unsigned opt_proxy_groups_current_;
  /**
   * Number of proxy servers that failed within current load-balance group.
   * Between 0 and (*opt_proxy_groups_)[opt_proxy_groups_current_].size().
   */
  unsigned opt_proxy_groups_current_burned_;
  /**
   * The index of the first fallback proxy group.  If there are none,
   *  it is set to the number of regular proxy groups.
   */
  unsigned opt_proxy_groups_fallback_;
  /**
   * Overall number of proxies summed over all the groups.
   */
  unsigned opt_num_proxies_;
  /**
   * The original proxy list provided to SetProxyChain.
   */
  std::string opt_proxy_list_;
  /**
   * The original proxy fallback list provided to SetProxyChain.
   */
  std::string opt_proxy_fallback_list_;
  /**
   * Load-balancing map of currently active proxies
   */
  std::map<uint32_t, ProxyInfo *> opt_proxy_map_;
  /**
   * Sorted list of currently active proxy URLs (for log messages)
   */
  std::vector<std::string> opt_proxies_;
  /**
   * Shard requests across multiple proxies via consistent hashing
   */
  bool opt_proxy_shard_;

  /**
   * Sharding policy deciding which proxy should be chosen for each download
   * request
   *
   * Sharding policy is shared between all download managers. As such shared
   * pointers are used to allow for proper clean-up afterwards in the destructor
   * (We cannot assume the order in which the download managers are stopped)
   */
  SharedPtr<ShardingPolicy> sharding_policy_;
  /**
   * Health check for the proxies
   *
   * Health check is shared between all download managers. As such shared
   * pointers are used to allow for proper clean-up afterwards in the destructor
   * (We cannot assume the order in which the download managers are stopped)
   */
  SharedPtr<HealthCheck> health_check_;
  /**
   * Endless retries for a failed download (hard failures will result in abort)
  */
  bool failover_indefinitely_;
  /**
   * Repo name. Needed for the re-try logic if a download was unsuccessful
   * Used in sharding policy && Interrupted()
   */
  std::string fqrn_;

  /**
   * Name of the download manager (default is "standard")
   */
  std::string name_;

  /**
   * Used to resolve proxy addresses (host addresses are resolved by the proxy).
   */
  dns::NormalResolver *resolver_;

  /**
   * If a proxy has IPv4 and IPv6 addresses, which one to prefer
   */
  dns::IpPreference opt_ip_preference_;

  /**
   * Used to replace @proxy@ in the Geo-API calls to order Stratum 1 servers,
   * in case the active proxy is DIRECT (no proxy).  Should be a UUID
   * identifying the host.
   */
  std::string proxy_template_direct_;
  /**
   * Used to force a value for @proxy@ in the Geo-API calls to order Stratum 1
   * servers.  If empty, the fully qualified domain name of the active proxy
   * server is used.
   */
  std::string proxy_template_forced_;

  /**
   * More than one proxy group can be considered as group of primary proxies
   * followed by backup proxy groups, e.g. at another site.
   * If opt_proxy_groups_reset_after_ is > 0, cvmfs will reset its proxy group
   * to the first one after opt_proxy_groups_reset_after_ seconds are elapsed.
   */
  time_t opt_timestamp_backup_proxies_;
  time_t opt_timestamp_failover_proxies_;  // failover within the same group
  unsigned opt_proxy_groups_reset_after_;

  CredentialsAttachment *credentials_attachment_;

  /**
   * Writes and reads should be atomic because reading happens in a different
   * thread than writing.
   */
  Counters *counters_;

  /**
   * Carries the path settings for SSL certificates
   */
  SslCertificateStore ssl_certificate_store_;
};  // DownloadManager

}  // namespace download

#endif  // CVMFS_NETWORK_DOWNLOAD_H_
