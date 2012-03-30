/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_DOWNLOAD_H_
#define CVMFS_DOWNLOAD_H_

#include <stdint.h>
#include <cstdio>
#include <string>
#include <vector>

#include "duplex_curl.h"
#include "compression.h"
#include "hash.h"

namespace download {

/**
 * Where to store downloaded data.
 */
enum Destination {
  kDestinationMem = 1,
  kDestinationFile,
  kDestinationPath
};

/**
 * Possible return values.
 */
enum Failures {
  kFailOk = 0,
  kFailLocalIO,
  kFailBadUrl,
  kFailProxyConnection,
  kFailHostConnection,
  kFailBadData,
  kFailOther,
};

/**
 * Contains all the information to specify a download job.
 */
struct JobInfo {
  const std::string *url;
  bool compressed;
  bool probe_hosts;
  Destination destination;
  struct {
    size_t size;
    size_t pos;
    char *data;
  } destination_mem;
  FILE *destination_file;
  const std::string *destination_path;
  const hash::Any *expected_hash;

  // One constructor per destination
  JobInfo() { wait_at[0] = wait_at[1] = -1; }
  JobInfo(const std::string *u, const bool c, const bool ph,
          const std::string *p, const hash::Any *h) : url(u), compressed(c),
          probe_hosts(ph), destination(kDestinationPath), destination_path(p),
          expected_hash(h)
          { wait_at[0] = wait_at[1] = -1; }
  JobInfo(const std::string *u, const bool c, const bool ph, FILE *f,
          const hash::Any *h) : url(u), compressed(c), probe_hosts(ph),
          destination(kDestinationFile), destination_file(f), expected_hash(h)
          { wait_at[0] = wait_at[1] = -1; }
  JobInfo(const std::string *u, const bool c, const bool ph,
          const hash::Any *h) : url(u), compressed(c), probe_hosts(ph),
          destination(kDestinationMem), expected_hash(h)
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
  hash::ContextPtr hash_context;
  int wait_at[2];  /**< Pipe used for the return value */
  std::string proxy;
  bool nocache;
  Failures error_code;
  unsigned char num_failed_proxies;
  unsigned char num_failed_hosts;
};


void Init(const unsigned max_pool_handles);
void Fini();
void Spawn();
Failures Fetch(JobInfo *info);

void SetDnsServer(const std::string &address);
void SetTimeout(const unsigned seconds_proxy, const unsigned seconds_direct);
void GetTimeout(unsigned *seconds_proxy, unsigned *seconds_direct);
uint64_t GetTransferredBytes();
uint64_t GetTransferTime();
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
void RestartNetwork();

}  // namespace download

#endif  // CVMFS_DOWNLOAD_H_
