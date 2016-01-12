/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_DOWNLOAD_H_
#define CVMFS_DOWNLOAD_H_

#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <unistd.h>

#include <cstdio>
#include <set>
#include <string>
#include <vector>

#include "gtest/gtest_prod.h"

#include "atomic.h"
#include "compression.h"
#include "dns.h"
#include "duplex_curl.h"
#include "hash.h"
#include "prng.h"
#include "sink.h"
#include "statistics.h"


namespace download {

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
  kFailTooBig,
  kFailOther,
  kFailUnsupportedProtocol,

  kFailNumEntries
};  // Failures


inline const char *Code2Ascii(const Failures error) {
  const char *texts[kFailNumEntries + 1];
  texts[0] = "OK";
  texts[1] = "local I/O failure";
  texts[2] = "malformed URL";
  texts[3] = "failed to resolve proxy address";
  texts[4] = "failed to resolve host address";
  texts[5] = "all proxies failed, trying host fail-over";
  texts[6] = "proxy connection problem";
  texts[7] = "host connection problem";
  texts[8] = "proxy returned HTTP error";
  texts[9] = "host returned HTTP error";
  texts[10] = "corrupted data received";
  texts[11] = "resource too big to download";
  texts[12] = "unknown network error";
  texts[13] = "Unsupported URL in protocol";
  texts[14] = "no text";
  return texts[error];
}


/**
 * Where to store downloaded data.
 */
enum Destination {
  kDestinationMem = 1,
  kDestinationFile,
  kDestinationPath,
  kDestinationSink,
  kDestinationNone
};  // Destination


struct Counters {
  perf::Counter *sz_transferred_bytes;
  perf::Counter *sz_transfer_time;  // measured in miliseconds
  perf::Counter *n_requests;
  perf::Counter *n_retries;
  perf::Counter *n_proxy_failover;
  perf::Counter *n_host_failover;

  Counters(perf::Statistics *statistics, const std::string &name) {
    sz_transferred_bytes = statistics->Register(name + ".sz_transferred_bytes",
        "Number of transferred bytes");
    sz_transfer_time = statistics->Register(name + ".sz_transfer_time",
        "Transfer time (miliseconds)");
    n_requests = statistics->Register(name + ".n_requests",
        "Number of requests");
    n_retries = statistics->Register(name + ".n_retries", "Number of retries");
    n_proxy_failover = statistics->Register(name + ".n_proxy_failover",
        "Number of proxy failovers");
    n_host_failover = statistics->Register(name + ".n_host_failover",
        "Number of host failovers");
  }
};  // Counters


/**
 * Contains all the information to specify a download job.
 */
struct JobInfo {
  const std::string *url;
  bool compressed;
  bool probe_hosts;
  bool head_request;
  bool follow_redirects;
  pid_t pid;
  uid_t uid;
  gid_t gid;
  char *cred_fname;  // TODO(bbockelm): This is a fallback method -
                     // can we eliminate?
  void *cred_data;  // Per-transfer credential data
  Destination destination;
  struct {
    size_t size;
    size_t pos;
    char *data;
  } destination_mem;
  FILE *destination_file;
  const std::string *destination_path;
  cvmfs::Sink *destination_sink;
  const shash::Any *expected_hash;
  const std::string *extra_info;

  // Allow byte ranges to be specified.
  off_t range_offset;
  off_t range_size;

  // Default initialization of fields
  void Init() {
    url = NULL;
    compressed = false;
    probe_hosts = false;
    head_request = false;
    follow_redirects = false;
    pid = -1;
    uid = -1;
    gid = -1;
    cred_fname = NULL;
    cred_data = NULL;
    destination = kDestinationNone;
    destination_mem.size = destination_mem.pos = 0;
    destination_mem.data = NULL;
    destination_file = NULL;
    destination_path = NULL;
    destination_sink = NULL;
    expected_hash = NULL;
    extra_info = NULL;

    curl_handle = NULL;
    headers = NULL;
    memset(&zstream, 0, sizeof(zstream));
    info_header = NULL;
    wait_at[0] = wait_at[1] = -1;
    nocache = false;
    error_code = kFailOther;
    num_used_proxies = num_used_hosts = num_retries = 0;
    backoff_ms = 0;

    range_offset = -1;
    range_size = -1;
    http_code = -1;
  }

  // One constructor per destination + head request
  JobInfo() { Init(); }
  JobInfo(const std::string *u, const bool c, const bool ph,
          const std::string *p, const shash::Any *h)
  {
    Init();
    url = u;
    compressed = c;
    probe_hosts = ph;
    destination = kDestinationPath;
    destination_path = p;
    expected_hash = h;
  }
  JobInfo(const std::string *u, const bool c, const bool ph, FILE *f,
          const shash::Any *h)
  {
    Init();
    url = u;
    compressed = c;
    probe_hosts = ph;
    destination = kDestinationFile;
    destination_file = f;
    expected_hash = h;
  }
  JobInfo(const std::string *u, const bool c, const bool ph,
          const shash::Any *h)
  {
    Init();
    url = u;
    compressed = c;
    probe_hosts = ph;
    destination = kDestinationMem;
    expected_hash = h;
  }
  JobInfo(const std::string *u, const bool c, const bool ph,
          cvmfs::Sink *s, const shash::Any *h)
  {
    Init();
    url = u;
    compressed = c;
    probe_hosts = ph;
    destination = kDestinationSink;
    destination_sink = s;
    expected_hash = h;
  }
  JobInfo(const std::string *u, const bool ph) {
    Init();
    url = u;
    probe_hosts = ph;
    head_request = true;
  }

  ~JobInfo() {
    delete cred_fname;
    if (wait_at[0] >= 0) {
      close(wait_at[0]);
      close(wait_at[1]);
    }
  }

  // Internal state, don't touch
  CURL *curl_handle;
  curl_slist *headers;
  char *info_header;
  z_stream zstream;
  shash::ContextPtr hash_context;
  int wait_at[2];  /**< Pipe used for the return value */
  std::string proxy;
  bool nocache;
  Failures error_code;
  int http_code;
  unsigned char num_used_proxies;
  unsigned char num_used_hosts;
  unsigned char num_retries;
  unsigned backoff_ms;
};  // JobInfo


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


class DownloadManager {
  FRIEND_TEST(T_Download, ValidateGeoReply);
  FRIEND_TEST(T_Download, StripDirect);

 public:
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

  /**
   * Do not download files larger than 1M into memory.
   */
  static const unsigned kMaxMemSize;

  static const unsigned kDnsDefaultRetries = 1;
  static const unsigned kDnsDefaultTimeoutMs = 3000;

  DownloadManager();
  ~DownloadManager();

  static int ParseHttpCode(const char digits[3]);

  void Init(const unsigned max_pool_handles, const bool use_system_proxy,
      perf::Statistics * statistics, const std::string &name = "download");
  void Fini();
  void Spawn();
  Failures Fetch(JobInfo *info);

  void SetDnsServer(const std::string &address);
  void SetDnsParameters(const unsigned retries, const unsigned timeout_ms);
  void SetIpPreference(const dns::IpPreference preference);
  void SetTimeout(const unsigned seconds_proxy, const unsigned seconds_direct);
  void GetTimeout(unsigned *seconds_proxy, unsigned *seconds_direct);
  void SetLowSpeedLimit(const unsigned low_speed_limit);
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
  void RebalanceProxies();
  void SwitchProxyGroup();
  void SetProxyGroupResetDelay(const unsigned seconds);
  void SetHostResetDelay(const unsigned seconds);
  void SetRetryParameters(const unsigned max_retries,
                          const unsigned backoff_init_ms,
                          const unsigned backoff_max_ms);
  void SetMaxIpaddrPerProxy(unsigned limit);
  void SetProxyTemplates(const std::string &direct, const std::string &forced);
  void EnableInfoHeader();
  void EnablePipelining();
  void EnableRedirects();

 private:
  static int CallbackCurlSocket(CURL *easy, curl_socket_t s, int action,
                                void *userp, void *socketp);
  static void *MainDownload(void *data);

  bool StripDirect(const std::string &proxy_list, std::string *cleaned_list);
  bool ValidateGeoReply(const std::string &reply_order,
                        const unsigned expected_size,
                        std::vector<uint64_t> *reply_vals);
  void SwitchHost(JobInfo *info);
  void SwitchProxy(JobInfo *info);
  void RebalanceProxiesUnlocked();
  CURL *AcquireCurlHandle();
  void ReleaseCurlHandle(CURL *handle);
  void InitializeRequest(JobInfo *info, CURL *handle);
  void SetUrlOptions(JobInfo *info);
  void ValidateProxyIpsUnlocked(const std::string &url, const dns::Host &host);
  void UpdateStatistics(CURL *handle);
  bool CanRetry(const JobInfo *info);
  void Backoff(JobInfo *info);
  bool VerifyAndFinalize(const int curl_error, JobInfo *info);
  void InitHeaders();
  void FiniHeaders();

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
  int pipe_terminate_[2];

  int pipe_jobs_[2];
  struct pollfd *watch_fds_;
  uint32_t watch_fds_size_;
  uint32_t watch_fds_inuse_;
  uint32_t watch_fds_max_;

  pthread_mutex_t *lock_options_;
  pthread_mutex_t *lock_synchronous_mode_;
  char *opt_dns_server_;
  unsigned opt_timeout_proxy_;
  unsigned opt_timeout_direct_;
  unsigned opt_low_speed_limit_;
  unsigned opt_max_retries_;
  unsigned opt_backoff_init_ms_;
  unsigned opt_backoff_max_ms_;
  bool enable_info_header_;
  bool opt_ipv4_only_;
  bool follow_redirects_;

  // Host list
  std::vector<std::string> *opt_host_chain_;
  /**
   * Created by SetHostChain(), filled by probe_hosts.  Contains time to get
   * .cvmfschecksum in ms. -1 is unprobed, -2 is error.
   */
  std::vector<int> *opt_host_chain_rtt_;
  unsigned opt_host_chain_current_;

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

  /**
   * Similarly to proxy group reset, we'd also like to reset the host after a
   * failover.  Host outages can last longer and might come with a separate
   * reset delay.
   */
  time_t opt_timestamp_backup_host_;
  unsigned opt_host_reset_after_;

  // Writes and reads should be atomic because reading happens in a different
  // thread than writing.
  Counters *counters_;
};  // DownloadManager

}  // namespace download

#endif  // CVMFS_DOWNLOAD_H_
