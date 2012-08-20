//all: test

//test: test.cc download.cc download.h curl_duplex.h logging.cc logging.h logging-internal.h smalloc.h smalloc.c compression.c compression.h zlib-duplex.h sha1.c sha1.h debug.c debug.h hash.cc hash.h md5.h cvmfs_config.h util.cc util.h compat.h compat_linux.h compat_macosx.h
//        gcc -c -O2 smalloc.c
//        gcc -c -O2 compression.c
//        gcc -c -O2 sha1.c
//        gcc -c -O2 debug.c
//        g++ -g -Wall -D_BUILT_IN_LIBCURL -DDEBUGMSG -O0 -o test test.cc download.cc util.cc logging.cc hash.cc sha1.o debug.o compression.o smalloc.o curl-7.24.0/lib/.libs/libcurl.a ../V2_1/build/externals/c-ares/src/.libs/libcares.a -lz -lssl -lrt


#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>

#include <cassert>
#include <cstdio>
#include <cstring>

#include <string>
#include <vector>

#include "download.h"
#include "logging.h"
#include "hash.h"
#include "util.h"

extern "C" {
#include "smalloc.h"
}

using namespace std;

static void Run() {
  LogCvmfs(kLogDownload, kLogStdout, "Downloading file into path");
  string url_published = "http://cvmappi10.cern.ch/opt/atlas/.cvmfspublished";
  string destination = "/tmp/testdownload";
  download::JobInfo info(&url_published,  false, false, &destination, NULL);
  int retval = download::Fetch(&info);
  assert(retval == download::kFailOk);

  LogCvmfs(kLogDownload, kLogStdout, "Downloading file into file");
  info.destination = download::kDestinationFile;
  info.destination_file = fopen("/tmp/testdownload.file", "w+");
  retval = download::Fetch(&info);
  assert(retval == download::kFailOk);
  assert(fclose(info.destination_file) == 0);

  LogCvmfs(kLogDownload, kLogStdout, "Downloading file into memory");
  info.destination = download::kDestinationMem;
  retval = download::Fetch(&info);
  assert(retval == download::kFailOk);

  LogCvmfs(kLogDownload, kLogStdout, "Compare");
  void *compare = smalloc(info.destination_mem.size);
  FILE *f = fopen("/tmp/testdownload", "r");
  assert(f);
  assert(fread(compare, 1, info.destination_mem.size, f) ==
         info.destination_mem.size);
  assert(memcmp(compare, info.destination_mem.data, info.destination_mem.size)
         == 0);
  fclose(f);
  f = fopen("/tmp/testdownload.file", "r");
  assert(fread(compare, 1, info.destination_mem.size, f) ==
         info.destination_mem.size);
  assert(memcmp(compare, info.destination_mem.data, info.destination_mem.size)
         == 0);

  fclose(f);
  free(info.destination_mem.data);
  info.destination_mem.data = NULL;

  LogCvmfs(kLogDownload, kLogStdout, "Unsupported Protocol");
  string unsupported_url = "https://cvmappi10.cern.ch/opt/atlas/.cvmfspublished";
  info.url = &unsupported_url;
  retval = download::Fetch(&info);
  assert(retval == download::kFailBadUrl);

  //  LogCvmfs(kLogDownload, kLogStdout, "Bad URL");
  //  info.url = "blablabla";
  //  retval = download::Fetch(&info);
  //  assert(retval == download::kFailBadUrl);

  LogCvmfs(kLogDownload, kLogStdout, "Decompression Failure (Memory)");
  string url = "http://cvmappi10.cern.ch/opt/atlas/.cvmfspublished";
  info.url = &url;
  info.compressed = true;
  retval = download::Fetch(&info);
  assert(retval == download::kFailBadData);

  LogCvmfs(kLogDownload, kLogStdout, "Decompression Failure (File)");
  info.destination = download::kDestinationPath;
  retval = download::Fetch(&info);
  assert(retval == download::kFailBadData);

  /*LogCvmfs(kLogDownload, kLogStdout, "Decompression");
  info.compressed = false;
  info.destination = download::kDestinationMem;
  retval = download::Fetch(&info);
  assert(retval == download::kFailOk);
  string hash(((char *)info.destination_mem.data)+1, 40);
  string caturl = "http://cvmappi10.cern.ch/opt/atlas/data/" +
                  hash.substr(0, 2) + "/" + hash.substr(2) + "C";
  info.url = &caturl;
  info.compressed = true;
  info.destination = download::kDestinationPath;
  hash::t_sha1 sha1;
  sha1.from_hash_str(hash);
  info.expected_hash = &sha1;
  retval = download::Fetch(&info);
  assert(retval == download::kFailOk);

  LogCvmfs(kLogDownload, kLogStdout, "Hash verification failure");
  hash::t_sha1 sha1_null;
  info.expected_hash = &sha1_null;
  retval = download::Fetch(&info);
  assert(retval == download::kFailBadData);*/
}

void *MultiThreadedRun(void *data) {
  LogCvmfs(kLogDownload, kLogStdout, "Running thread %p", pthread_self());
  string url_published = "http://cvmappi10.cern.ch/opt/atlas/.cvmfspublished";
  download::JobInfo info(&url_published, false, false, NULL);
  for (int i = 0; i < 1000; ++i) {
    LogCvmfs(kLogDownload, kLogStdout, "Downloading file into memory");
    int retval = download::Fetch(&info);
    assert(retval == download::kFailOk);
    free(info.destination_mem.data);
    info.destination_mem.data = NULL;
  }

  return NULL;
}


void *ThreadDnsSink(void *data) {
  const int MSGBUFSIZE = 512;
  int fd_recv;
  struct sockaddr_in addr_this;
  int retval;

  fd_recv = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP);
  assert(fd_recv >= 0);

  memset(&addr_this, 0, sizeof(addr_this));
  addr_this.sin_family = AF_INET;
  addr_this.sin_addr.s_addr = inet_addr("127.0.0.1");
  addr_this.sin_port = htons(53);

  retval = bind(fd_recv, (struct sockaddr *)&addr_this, sizeof(addr_this));
  assert(retval == 0);

  char msgbuf[MSGBUFSIZE];
  struct sockaddr_in addr_sender;
  socklen_t addr_sender_len = sizeof(addr_sender);
  int nbytes;
  LogCvmfs(kLogDownload, kLogStdout, "Starting DNS Sink");
  while (1) {
    nbytes = recvfrom(fd_recv, msgbuf, MSGBUFSIZE-1, 0,
                      (struct sockaddr *)&addr_sender, &addr_sender_len);
    if (nbytes < 0)
      break;

    if (nbytes > 0)
    {
      msgbuf[nbytes] = 0;
      LogCvmfs(kLogDownload, kLogStdout, "DNS sink received %s", msgbuf);
    } else {
      LogCvmfs(kLogDownload, kLogStdout, "DNS sink received 0 bytes", msgbuf);
    }
  }

  return NULL;
}


void *ThreadHttpConnectSink(void *data) {
  const int MSGBUFSIZE = 512;
  int fd_recv;
  struct sockaddr_in addr_this;
  int retval;

  fd_recv = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
  assert(fd_recv >= 0);

  memset(&addr_this, 0, sizeof(addr_this));
  addr_this.sin_family = AF_INET;
  addr_this.sin_addr.s_addr = inet_addr("127.0.0.1");
  addr_this.sin_port = htons(80);

  retval = bind(fd_recv, (struct sockaddr *)&addr_this, sizeof(addr_this));
  assert(retval == 0);

  retval = listen(fd_recv, 1);
  assert(retval == 0);

  char msgbuf[MSGBUFSIZE];
  struct sockaddr_in addr_sender;
  socklen_t addr_sender_len = sizeof(addr_sender);
  int nbytes;
  LogCvmfs(kLogDownload, kLogStdout, "Starting Http Connect Sink");
  while (1) {
    nbytes = recvfrom(fd_recv, msgbuf, MSGBUFSIZE-1, 0,
                      (struct sockaddr *)&addr_sender, &addr_sender_len);
    if (nbytes < 0)
      break;

    if (nbytes > 0)
    {
      msgbuf[nbytes] = 0;
      LogCvmfs(kLogDownload, kLogStdout, "Http connect sink received %s", msgbuf);
    } else {
      LogCvmfs(kLogDownload, kLogStdout, "Http connect sink received 0 bytes", msgbuf);
    }
  }

  return NULL;
}


void *ThreadHttpSink(void *data) {
  const int MSGBUFSIZE = 512;
  int fd_recv;
  struct sockaddr_in addr_this;
  int retval;

  fd_recv = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
  assert(fd_recv >= 0);

  memset(&addr_this, 0, sizeof(addr_this));
  addr_this.sin_family = AF_INET;
  addr_this.sin_addr.s_addr = inet_addr("127.0.0.1");
  addr_this.sin_port = htons(81);

  retval = bind(fd_recv, (struct sockaddr *)&addr_this, sizeof(addr_this));
  assert(retval == 0);

  retval = listen(fd_recv, 1);
  assert(retval == 0);

  struct sockaddr sender;
  socklen_t address_len;

  LogCvmfs(kLogDownload, kLogStdout, "Starting Http Sink");
  while (1) {
    int conn_fd = accept(fd_recv, &sender, &address_len);
    assert(conn_fd >= 0);

    LogCvmfs(kLogDownload, kLogStdout, "New connection Sink");

    char msgbuf[MSGBUFSIZE];
    int nbytes;
    nbytes = read(conn_fd, msgbuf, MSGBUFSIZE-1);
    if (nbytes < 0)
      break;

    if (nbytes > 0)
    {
      msgbuf[nbytes] = 0;
      LogCvmfs(kLogDownload, kLogStdout, "Http sink received %s", msgbuf);
    } else {
      LogCvmfs(kLogDownload, kLogStdout, "Http sink received 0 bytes", msgbuf);
    }
  }

  return NULL;
}


void TestTimeout() {
  LogCvmfs(kLogDownload, kLogStdout, "DNS Timeout");
  string url = "http://cvmappi10.cern.ch/opt/atlas/.cvmfspublished";
  download::JobInfo info(&url, false, false, NULL);
  time_t start = time(NULL);
  int retval = download::Fetch(&info);
  time_t end = time(NULL);
  assert((retval == download::kFailHostConnection) && (end-start < 2));

  /*LogCvmfs(kLogDownload, kLogStdout, "Access by IP");
  string url_ip = "http://137.138.234.36/opt/atlas/.cvmfspublished";
  info.url = &url_ip;
  retval = download::Fetch(&info);
  assert(retval == download::kFailOk);*/

  LogCvmfs(kLogDownload, kLogStdout, "Http Connection Timeout");
  string url_local = "http://127.0.0.1/opt/atlas/.cvmfspublished";
  info.url = &url_local;
  start = time(NULL);
  retval = download::Fetch(&info);
  end = time(NULL);
  assert((retval == download::kFailHostConnection) && (end-start < 3));

  LogCvmfs(kLogDownload, kLogStdout, "Http Timeout");
  url_local = "http://127.0.0.1:81/opt/atlas/.cvmfspublished";
  start = time(NULL);
  retval = download::Fetch(&info);
  end = time(NULL);
  assert((retval == download::kFailHostConnection) && (end-start < 3));

  LogCvmfs(kLogDownload, kLogStdout, "Proxy Connection Timeout");
  download::SetProxyChain("http://127.0.0.1:80");
  start = time(NULL);
  retval = download::Fetch(&info);
  end = time(NULL);
  assert((retval == download::kFailProxyConnection) && (end-start < 3));

  LogCvmfs(kLogDownload, kLogStdout, "Proxy Timeout");
  download::SetProxyChain("http://127.0.0.1:81");
  start = time(NULL);
  retval = download::Fetch(&info);
  end = time(NULL);
  assert((retval == download::kFailProxyConnection) && (end-start < 3));
}

void TestFailureHandling() {
  int retval;
  //LogCvmfs(kLogDownload, kLogStdout, "Bad Data");
  string url = "http://cvmappi10.cern.ch/opt/atlas/.cvmfspublished";
  hash::t_sha1 sha1;
  download::JobInfo info(&url, false, false, &sha1);
  retval = download::Fetch(&info);
  assert((retval == download::kFailBadData) && (info.nocache));

  LogCvmfs(kLogDownload, kLogStdout, "Switch Host");
  url = "/opt/atlas/.cvmfspublished";
  info.expected_hash = NULL;
  info.probe_hosts = true;
  download::SetHostChain("http://A;http://cvmappi10.cern.ch");
  retval = download::Fetch(&info);
  assert((retval == download::kFailOk) &&
         (info.num_failed_hosts == 1) && (info.num_failed_proxies == 0));

  LogCvmfs(kLogDownload, kLogStdout, "Switch Host Full");
  download::SetHostChain("http://A;http://B");
  retval = download::Fetch(&info);
  assert((retval == download::kFailHostConnection) &&
         (info.num_failed_hosts == 2) && (info.num_failed_proxies == 0));

  LogCvmfs(kLogDownload, kLogStdout, "Switch Proxy");
  download::SetProxyChain("http://A;http://B;http://C|http://D");
  retval = download::Fetch(&info);
  assert((retval == download::kFailProxyConnection) &&
         (info.num_failed_hosts == 0) && (info.num_failed_proxies == 4));

  download::SetHostChain("http://cvmappi10.cern.ch");
  download::SetProxyChain("http://cmst0frontier.cern.ch:3128");
  retval = download::Fetch(&info);
  assert(retval == download::kFailProxyConnection);

  download::SetHostChain("http://cvmfs-stratum-one.cern.ch");
  download::SetProxyChain("http://cmst0frontier.cern.ch:3128|http://ca-proxy.cern.ch:3128");
  retval = download::Fetch(&info);
  assert(retval == download::kFailOk);

  //download::SetHostChain("http://A;http://cvmfs-stratum-one.cern.ch");
  //download::SetProxyChain("http://cmst0frontier.cern.ch:3128|http://ca-proxy.cern.ch:3128");
  //retval = download::Fetch(&info);
  //assert(retval == download::kFailOk);
}


void ShowProxies(const vector< vector<string> > &chain,
                 const unsigned lb_group)
{
  for (unsigned i = 0; i < chain.size(); ++i) {
    LogCvmfs(kLogDownload, kLogStdout, "Group %u: %s",
             i, join_strings(chain[i], ", ").c_str());
  }
  LogCvmfs(kLogDownload, kLogStdout, "Current group: %u", lb_group);
}


void ShowHosts(const vector<string> &chain, const vector<int> &rtt,
               const unsigned active)
{
  for (unsigned i = 0; i < chain.size(); ++i) {
    LogCvmfs(kLogDownload, kLogStdout, "Host %u (%d): %s",
             i, rtt[i], chain[i].c_str());
  }
  LogCvmfs(kLogDownload, kLogStdout, "Current host: %s", chain[active].c_str());
}


int main() {

  pthread_t dns_sink;
  int retval = pthread_create(&dns_sink, NULL, ThreadDnsSink, NULL);
  assert(retval == 0);
  pthread_t http_connect_sink;
  retval = pthread_create(&http_connect_sink, NULL, ThreadHttpConnectSink, NULL);
  assert(retval == 0);
  pthread_t http_sink;
  retval = pthread_create(&http_sink, NULL, ThreadHttpSink, NULL);
  assert(retval == 0);

  download::Init(32);


  TestFailureHandling();
  download::SetProxyChain("");
  download::SetHostChain("");
  Run();
  TestFailureHandling();

  download::Spawn();
  download::SetProxyChain("");
  download::SetHostChain("");
  TestFailureHandling();
  download::SetProxyChain("");
  download::SetHostChain("");

  Run();

  MultiThreadedRun(NULL);

  const int num_workers = 32;
  pthread_t workers[num_workers];
  for (int i = 0; i < num_workers; ++i) {
    pthread_create(&workers[i], NULL, MultiThreadedRun, NULL);
  }
  for (int i = 0; i < num_workers; ++i) {
    pthread_join(workers[i], NULL);
  }

  LogCvmfs(kLogDownload, kLogStdout, "Transferred %ld bytes",
           download::GetTransferredBytes());
  LogCvmfs(kLogDownload, kLogStdout, "Transfer time: %ld",
           download::GetTransferTime());
  LogCvmfs(kLogDownload, kLogStdout, "Kb/s: %ld",
           (download::GetTransferredBytes()/1024)/download::GetTransferTime());
  download::Fini();

  download::Init(10);
  LogCvmfs(kLogDownload, kLogStdout, "Setting / Switching Proxy Servers");
  vector< vector<string> > proxy_chain;
  unsigned current_group;
  download::SetProxyChain("A");
  download::GetProxyInfo(&proxy_chain, &current_group);
  ShowProxies(proxy_chain, current_group);
  download::SetProxyChain("A;B;C");
  download::GetProxyInfo(&proxy_chain, &current_group);
  ShowProxies(proxy_chain, current_group);
  download::SetProxyChain("A|B|C");
  download::GetProxyInfo(&proxy_chain, &current_group);
  ShowProxies(proxy_chain, current_group);
  download::SetProxyChain(";;;");
  download::GetProxyInfo(&proxy_chain, &current_group);
  ShowProxies(proxy_chain, current_group);
  download::SetProxyChain(";||;");
  download::GetProxyInfo(&proxy_chain, &current_group);
  ShowProxies(proxy_chain, current_group);
  download::SetProxyChain("A|B|C;D|E");
  download::GetProxyInfo(&proxy_chain, &current_group);
  ShowProxies(proxy_chain, current_group);
  download::SwitchProxyGroup();
  download::GetProxyInfo(&proxy_chain, &current_group);
  ShowProxies(proxy_chain, current_group);
  download::RebalanceProxies();
  download::GetProxyInfo(&proxy_chain, &current_group);
  ShowProxies(proxy_chain, current_group);
  download::RebalanceProxies();
  download::GetProxyInfo(&proxy_chain, &current_group);
  ShowProxies(proxy_chain, current_group);
  download::RebalanceProxies();
  download::GetProxyInfo(&proxy_chain, &current_group);
  ShowProxies(proxy_chain, current_group);
  download::SetProxyChain("");

  LogCvmfs(kLogDownload, kLogStdout, "Probing / Switching Hosts");
  vector<string> host_chain;
  vector<int> rtt;
  unsigned active_host;
  download::SetHostChain("A;B;C");
  download::GetHostInfo(&host_chain, &rtt, &active_host);
  ShowHosts(host_chain, rtt, active_host);
  download::ProbeHosts();
  download::GetHostInfo(&host_chain, &rtt, &active_host);
  ShowHosts(host_chain, rtt, active_host);
  download::SetHostChain("http://cvmfs-stratum-one.cern.ch/opt/atlas;http://cvmfs-stratum-zero.cern.ch/opt/atlas");
  download::ProbeHosts();
  download::GetHostInfo(&host_chain, &rtt, &active_host);
  ShowHosts(host_chain, rtt, active_host);
  download::SwitchHost();
  download::GetHostInfo(&host_chain, &rtt, &active_host);
  ShowHosts(host_chain, rtt, active_host);
  download::Fini();

  download::Init(10);
  download::SetTimeout(1, 1);
  download::SetDnsServer("127.0.0.1");
  TestTimeout();
  download::Spawn();
  download::SetProxyChain("");
  download::SetHostChain("");
  TestTimeout();
  download::Fini();

  unlink("/tmp/testdownload");
  unlink("/tmp/testdownload.file");

  return 0;
}
