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
  kFailTooBig,
  kFailOther,
};  // Failures


inline const char *Code2Ascii(const Failures error) {
  const int kNumElems = 13;
  if (error >= kNumElems)
    return "no text available (internal error)";

  const char *texts[kNumElems];
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

  return texts[error];
}


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
  const std::string *extra_info;

  // Default initialization of fields
  void Init() {
    url = NULL;
    compressed = false;
    probe_hosts = false;
    head_request = false;
    destination = kDestinationNone;
    destination_mem.size = destination_mem.pos = 0;
    destination_mem.data = NULL;
    destination_file = NULL;
    destination_path = NULL;
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
  JobInfo(const std::string *u, const bool ph) {
    Init();
    url = u;
    probe_hosts = ph;
    head_request = true;
  }

  ~JobInfo() {
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
 public:
  /**
   * Do not download files larger than 1M into memory.
   */
  static const unsigned kMaxMemSize = 1024*1024;

  DownloadManager();
  ~DownloadManager();

  void Init(const unsigned max_pool_handles, const bool use_system_proxy);
  void Fini();
  void Spawn();
  Failures Fetch(JobInfo *info);

  void SetDnsServer(const std::string &address);
  void SetDnsParameters(const unsigned retries, const unsigned timeout_sec);
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
  void EnableInfoHeader();
  void EnablePipelining();
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
  bool enable_info_header_;
  bool opt_ipv4_only_;

  /**
   * Used to resolve proxy addresses (host addresses are resolved by the proxy).
   */
  dns::CaresResolver *resolver;

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
