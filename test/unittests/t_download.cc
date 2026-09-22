/**
 * This file is part of the CernVM File System.
 */

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>
#include <cstring>

#include "c_file_sandbox.h"
#include "c_http_server.h"
#include "compression/compression.h"
#include "crypto/hash.h"
#include "gtest/gtest.h"
#include "interrupt.h"
#include "network/download.h"
#include "network/sink.h"
#include "network/sink_mem.h"
#include "network/sink_file.h"
#include "statistics.h"
#include "util/file_guard.h"
#include "util/posix.h"
#include "util/prng.h"

using namespace std;  // NOLINT

namespace {

class TestInterruptCue : public InterruptCue {
 public:
  virtual bool IsCanceled() { return true; }
};

}  // anonymous namespace

namespace download {

class T_Download : public FileSandbox {
 public:
  T_Download()
      : FileSandbox(string(tmp_path) + "/server_dir")
      , download_mgr(8, perf::StatisticsTemplate("test", &statistics)) { }

 protected:
  virtual void SetUp() { CreateSandbox(); }

  virtual void TearDown() { RemoveSandbox(); }

  FILE *CreateTemporaryFile(std::string *path) const {
    return CreateTempFile(GetCurrentWorkingDirectory() + "/cvmfs_ut_download",
                          0600, "w+", path);
  }

  static const char tmp_path[];

  perf::Statistics statistics;
  DownloadManager download_mgr;
};

const char T_Download::tmp_path[] = "./cvmfs_ut_download";

class TestSink : public cvmfs::Sink {
 public:
  TestSink() : Sink(true) {
    FILE *f = CreateTempFile("./cvmfs_ut_download", 0600, "w+", &path);
    assert(f);
    fd = dup(fileno(f));
    assert(fd >= 0);
    fclose(f);
  }

  virtual int64_t Write(const void *buf, uint64_t size) {
    return write(fd, buf, size);
  }

  virtual int Reset() {
    int retval = ftruncate(fd, 0);
    assert(retval == 0);
    return 0;
  }

  virtual int Purge() { return Reset(); }

  virtual bool IsValid() { return fd >= 0; }

  int Flush() { return 0; }

  bool Reserve(size_t /*size*/) { return true; }

  bool RequiresReserve() { return false; }

  std::string Describe() {
    std::string result = "Test Sink for path " + path;
    result += " and " + StringifyInt(fd);
    return result;
  }

  ~TestSink() {
    close(fd);
    unlink(path.c_str());
  }

  int fd;
  string path;
};


//------------------------------------------------------------------------------


TEST_F(T_Download, LocalFile) {
  string dest_path;
  FILE *fdest = CreateTemporaryFile(&dest_path);
  ASSERT_TRUE(fdest != NULL);
  UnlinkGuard unlink_guard(dest_path);

  string src_path = GetAbsolutePath(GetSmallFile());
  string src_url = "file://" + src_path;

  cvmfs::FileSink filesink(fdest);
  JobInfo info(&src_url, false /* compressed */, false /* probe hosts */, NULL,
               &filesink);
  download_mgr.Fetch(&info);
  EXPECT_EQ(info.error_code(), kFailOk);
  fclose(fdest);
}

TEST_F(T_Download, RemoteFile) {
  string dest_path;
  FILE *fdest = CreateTemporaryFile(&dest_path);
  ASSERT_TRUE(fdest != NULL);
  UnlinkGuard unlink_guard(dest_path);

  MockFileServer file_server(8082, sandbox_path_);

  string src_path = GetSmallFile();
  string src_url = "http://127.0.0.1:8082/" + GetFileName(src_path);

  cvmfs::FileSink filesink(fdest);
  JobInfo info(&src_url, false /* compressed */, false /* probe hosts */, NULL,
               &filesink);
  download_mgr.Fetch(&info);
  EXPECT_EQ(file_server.num_processed_requests(), 1);
  EXPECT_EQ(info.error_code(), kFailOk);
  fclose(fdest);
}

TEST_F(T_Download, Clone) {
  DownloadManager *download_mgr_cloned = download_mgr.Clone(
      perf::StatisticsTemplate("x", &statistics), "cloned");

  string dest_path;
  FILE *fdest = CreateTemporaryFile(&dest_path);
  ASSERT_TRUE(fdest != NULL);
  UnlinkGuard unlink_guard(dest_path);
  char buf = '1';
  fwrite(&buf, 1, 1, fdest);
  fclose(fdest);

  string url = "file://" + dest_path;
  cvmfs::MemSink memsink;
  JobInfo info(&url, false /* compressed */, false /* probe hosts */, NULL,
               &memsink);
  download_mgr_cloned->Fetch(&info);
  ASSERT_EQ(info.error_code(), kFailOk);
  ASSERT_EQ(memsink.pos(), 1U);
  EXPECT_EQ(memsink.data()[0], '1');
  delete download_mgr_cloned;

  // Don't crash
  DownloadManager *dm = new DownloadManager(
      1, perf::StatisticsTemplate("h", &statistics));
  download_mgr_cloned = dm->Clone(perf::StatisticsTemplate("y", &statistics),
                                  "cloned");
  delete dm;
  delete download_mgr_cloned;
}


TEST_F(T_Download, Multiple) {
  string dest_path;
  FILE *fdest = CreateTemporaryFile(&dest_path);
  ASSERT_TRUE(fdest != NULL);
  UnlinkGuard unlink_guard(dest_path);

  string src_path = GetAbsolutePath(GetSmallFile());
  string src_url = "file://" + src_path;

  DownloadManager second_mgr(8,
                             perf::StatisticsTemplate("second", &statistics));

  cvmfs::FileSink filesink(fdest);
  JobInfo info(&src_url, false /* compressed */, false /* probe hosts */, NULL,
               &filesink);
  JobInfo info2(&src_url, false /* compressed */, false /* probe hosts */, NULL,
                &filesink);
  download_mgr.Fetch(&info);
  second_mgr.Fetch(&info2);
  EXPECT_EQ(info.error_code(), kFailOk);
  EXPECT_EQ(info2.error_code(), kFailOk);
  fclose(fdest);
}


TEST_F(T_Download, RemoteFile2Mem) {
  string src_path = GetSmallFile();
  string src_content = GetFileContents(src_path);

  MockFileServer file_server(8082, sandbox_path_);

  string url = "http://127.0.0.1:8082/" + GetFileName(src_path);
  cvmfs::MemSink memsink;
  JobInfo info(&url, false /* compressed */, false /* probe hosts */, NULL,
               &memsink);
  download_mgr.Fetch(&info);
  ASSERT_EQ(file_server.num_processed_requests(), 1);
  ASSERT_EQ(info.error_code(), kFailOk);
  ASSERT_EQ(memsink.pos(), src_content.length());
  EXPECT_STREQ(reinterpret_cast<char *>(memsink.data()), src_content.c_str());
}


TEST_F(T_Download, RemoteFileRedirect) {
  string src_path = GetSmallFile();
  string src_content = GetFileContents(src_path);

  MockFileServer file_server(8082, sandbox_path_);
  MockRedirectServer redirect_server(8083, "http://127.0.0.1:8082");

  string url = "http://127.0.0.1:8083/" + GetFileName(src_path);

  download_mgr.EnableRedirects();
  cvmfs::MemSink memsink;
  JobInfo info(&url, false /* compressed */, false /* probe hosts */, NULL,
               &memsink);
  download_mgr.Fetch(&info);
  ASSERT_EQ(file_server.num_processed_requests(), 1);
  ASSERT_EQ(redirect_server.num_processed_requests(), 1);
  ASSERT_EQ(info.error_code(), kFailOk);
  ASSERT_EQ(memsink.pos(), src_content.length());
  EXPECT_STREQ(reinterpret_cast<char *>(memsink.data()), src_content.c_str());
}

TEST_F(T_Download, RemoteFileSimpleProxy) {
  string src_path = GetSmallFile();
  string src_content = GetFileContents(src_path);

  MockProxyServer proxy_server(8083);
  MockFileServer file_server(8082, sandbox_path_);

  download_mgr.SetProxyChain("http://127.0.0.1:8083", "",
                             DownloadManager::kSetProxyRegular);
  string url = "http://127.0.0.1:8082/" + GetFileName(src_path);
  cvmfs::MemSink memsink;
  JobInfo info(&url, false /* compressed */, false /* probe hosts */, NULL,
               &memsink);
  download_mgr.Fetch(&info);
  ASSERT_EQ(proxy_server.num_processed_requests(), 1);
  ASSERT_EQ(file_server.num_processed_requests(), 1);
  ASSERT_EQ(info.error_code(), kFailOk);
  ASSERT_EQ(memsink.pos(), src_content.length());
  EXPECT_STREQ(reinterpret_cast<char *>(memsink.data()), src_content.c_str());
}

TEST_F(T_Download, RemoteFileProxyRedirect) {
  string src_path = GetSmallFile();
  string src_content = GetFileContents(src_path);

  MockProxyServer proxy_server(8084);
  MockRedirectServer redirect_server(8083, "http://127.0.0.1:8082");
  MockFileServer file_server(8082, sandbox_path_);

  download_mgr.SetProxyChain("http://127.0.0.1:8084", "",
                             DownloadManager::kSetProxyRegular);
  download_mgr.EnableRedirects();
  string url = "http://127.0.0.1:8083/" + GetFileName(src_path);
  cvmfs::MemSink memsink;
  JobInfo info(&url, false /* compressed */, false /* probe hosts */, NULL,
               &memsink);
  download_mgr.Fetch(&info);
  ASSERT_EQ(proxy_server.num_processed_requests(), 2);
  ASSERT_EQ(redirect_server.num_processed_requests(), 1);
  ASSERT_EQ(file_server.num_processed_requests(), 1);
  ASSERT_EQ(info.num_used_hosts(), 1);
  ASSERT_EQ(info.error_code(), kFailOk);
  ASSERT_EQ(memsink.pos(), src_content.length());
  EXPECT_STREQ(reinterpret_cast<char *>(memsink.data()), src_content.c_str());
}

TEST_F(T_Download, LocalFile2Mem) {
  string src_path = GetSmallFile();
  string src_content = GetFileContents(src_path);

  string url = "file://" + GetAbsolutePath(src_path);
  cvmfs::MemSink memsink;
  JobInfo info(&url, false /* compressed */, false /* probe hosts */, NULL,
               &memsink);
  download_mgr.Fetch(&info);
  ASSERT_EQ(info.error_code(), kFailOk);
  ASSERT_EQ(memsink.pos(), src_content.length());
  EXPECT_STREQ(reinterpret_cast<char *>(memsink.data()), src_content.c_str());
}

TEST_F(T_Download, RemoteFileSwitchHosts) {
  string src_path = GetSmallFile();
  string src_content = GetFileContents(src_path);

  MockFileServer file_server(8082, sandbox_path_);
  download_mgr.SetHostChain("http://127.0.0.1:8083;http://127.0.0.1:8082");
  string url = "/" + GetFileName(src_path);
  cvmfs::MemSink memsink;
  JobInfo info(&url, false /* compressed */, true /* probe hosts */, NULL,
               &memsink);
  download_mgr.Fetch(&info);
  ASSERT_EQ(file_server.num_processed_requests(), 1);
  ASSERT_EQ(info.num_used_hosts(), 2);
  ASSERT_EQ(info.error_code(), kFailOk);
  ASSERT_EQ(memsink.pos(), src_content.length());
  EXPECT_STREQ(reinterpret_cast<char *>(memsink.data()), src_content.c_str());
}

TEST_F(T_Download, CancelRequest) {
  string src_path = GetSmallFile();
  string src_content = GetFileContents(src_path);

  MockFileServer file_server(8082, sandbox_path_);
  download_mgr.SetHostChain("http://127.0.0.1:8083;http://127.0.0.1:8082");
  string url = "/" + GetFileName(src_path);
  cvmfs::MemSink memsink;
  JobInfo info(&url, false /* compressed */, true /* probe hosts */, NULL,
               &memsink);
  TestInterruptCue tci;
  info.SetInterruptCue(&tci);
  download_mgr.Fetch(&info);
  ASSERT_EQ(info.num_used_hosts(), 1);
  ASSERT_EQ(info.error_code(), kFailCanceled);
  EXPECT_EQ(NULL, memsink.data());
}

TEST_F(T_Download, RemoteFileSwitchHostsAfterRedirect) {
  string src_path = GetSmallFile();
  string src_content = GetFileContents(src_path);

  MockRedirectServer redirect_server(8083, "http://127.0.0.1:8084");
  MockFileServer file_server(8082, sandbox_path_);

  download_mgr.EnableRedirects();
  download_mgr.SetHostChain("http://127.0.0.1:8083;http://127.0.0.1:8082");
  string url = "/" + GetFileName(src_path);
  cvmfs::MemSink memsink;
  JobInfo info(&url, false /* compressed */, true /* probe hosts */, NULL,
               &memsink);
  download_mgr.Fetch(&info);
  ASSERT_EQ(info.num_used_hosts(), 2);
  ASSERT_EQ(info.error_code(), kFailOk);
  ASSERT_EQ(memsink.pos(), src_content.length());
  EXPECT_STREQ(reinterpret_cast<char *>(memsink.data()), src_content.c_str());
}

TEST_F(T_Download, RemoteFileSwitchProxies) {
  string src_path = GetSmallFile();
  int src_fd = open(src_path.c_str(), O_RDONLY);
  string src_content;
  SafeReadToString(src_fd, &src_content);
  close(src_fd);

  MockFileServer file_server(8082, sandbox_path_);
  MockProxyServer proxy_server(8083);
  download_mgr.SetProxyChain("http://127.0.0.1:8084;http://127.0.0.1:8083", "",
                             DownloadManager::kSetProxyRegular);

  string src_url = "http://127.0.0.1:8082/" + GetFileName(src_path);
  cvmfs::MemSink memsink;
  JobInfo info(&src_url, false /* compressed */, false /* probe hosts */, NULL,
               &memsink);
  download_mgr.Fetch(&info);
  ASSERT_EQ(file_server.num_processed_requests(), 1);
  ASSERT_EQ(proxy_server.num_processed_requests(), 1);
  ASSERT_EQ(info.num_used_proxies(), 2);
  ASSERT_EQ(info.error_code(), kFailOk);
  ASSERT_EQ(memsink.pos(), src_content.length());
  EXPECT_STREQ(reinterpret_cast<char *>(memsink.data()), src_content.c_str());
}

TEST_F(T_Download, RemoteFileEmpty) {
  string src_path = GetEmptyFile();

  MockFileServer file_server(8082, sandbox_path_);

  string src_url = "http://127.0.0.1:8082/" + GetFileName(src_path);
  cvmfs::MemSink memsink;
  JobInfo info(&src_url, false /* compressed */, false /* probe hosts */, NULL,
               &memsink);
  download_mgr.Fetch(&info);
  ASSERT_EQ(file_server.num_processed_requests(), 1);
  ASSERT_EQ(info.error_code(), kFailOk);
  ASSERT_EQ(memsink.pos(), 0U);
}

TEST_F(T_Download, LocalFile2Sink) {
  string dest_path;
  FILE *fdest = CreateTemporaryFile(&dest_path);
  ASSERT_TRUE(fdest != NULL);
  UnlinkGuard unlink_guard(dest_path);
  char buf = '1';
  fwrite(&buf, 1, 1, fdest);
  fflush(fdest);

  TestSink test_sink;
  string url = "file://" + dest_path;
  JobInfo info(&url, false /* compressed */, false /* probe hosts */,
               NULL /* expected hash */, &test_sink);
  download_mgr.Fetch(&info);
  EXPECT_EQ(info.error_code(), kFailOk);
  EXPECT_EQ(1, pread(test_sink.fd, &buf, 1, 0));
  EXPECT_EQ('1', buf);

  rewind(fdest);
  Prng prng;
  prng.InitLocaltime();
  unsigned N = 16 * 1024;
  unsigned size = N * sizeof(uint32_t);
  uint32_t rnd_buf[N];  // 64kB
  for (unsigned i = 0; i < N; ++i)
    rnd_buf[i] = prng.Next(2147483647);
  shash::Any checksum(shash::kMd5);
  EXPECT_TRUE(
      zlib::CompressMem2File(reinterpret_cast<const unsigned char *>(rnd_buf),
                             size, fdest, &checksum));
  fclose(fdest);

  TestSink test_sink2;
  JobInfo info2(&url, true /* compressed */, false /* probe hosts */,
                &checksum /* expected hash */, &test_sink2);
  download_mgr.Fetch(&info2);
  EXPECT_EQ(info2.error_code(), kFailOk);
  EXPECT_EQ(size, GetFileSize(test_sink2.path));

  uint32_t validation[N];
  EXPECT_EQ(static_cast<int>(size), pread(test_sink2.fd, &validation, size, 0));
  EXPECT_EQ(0, memcmp(validation, rnd_buf, size));
}


TEST_F(T_Download, StripDirect) {
  string cleaned = "FALSE";
  EXPECT_FALSE(download_mgr.StripDirect("", &cleaned));
  EXPECT_EQ("", cleaned);
  EXPECT_TRUE(download_mgr.StripDirect("DIRECT", &cleaned));
  EXPECT_EQ("", cleaned);
  EXPECT_TRUE(download_mgr.StripDirect("DIRECT;DIRECT", &cleaned));
  EXPECT_EQ("", cleaned);
  EXPECT_TRUE(download_mgr.StripDirect("DIRECT;DIRECT|DIRECT", &cleaned));
  EXPECT_EQ("", cleaned);
  EXPECT_TRUE(download_mgr.StripDirect("DIRECT;DIRECT|", &cleaned));
  EXPECT_EQ("", cleaned);
  EXPECT_TRUE(download_mgr.StripDirect(";", &cleaned));
  EXPECT_EQ("", cleaned);
  EXPECT_TRUE(download_mgr.StripDirect(";||;;;|||", &cleaned));
  EXPECT_EQ("", cleaned);
  EXPECT_FALSE(download_mgr.StripDirect("A|B", &cleaned));
  EXPECT_EQ("A|B", cleaned);
  EXPECT_FALSE(download_mgr.StripDirect("A|B;C|D;E|F|G", &cleaned));
  EXPECT_EQ("A|B;C|D;E|F|G", cleaned);
  EXPECT_TRUE(download_mgr.StripDirect("A|DIRECT;C|D;E|F;DIRECT", &cleaned));
  EXPECT_EQ("A;C|D;E|F", cleaned);
}


// ---------------------------------------------------------------------------
// Proxy chain normalisation: DIRECT is a last-resort tier, never a peer of a
// live proxy, and never silently dropped.
// ---------------------------------------------------------------------------

TEST_F(T_Download, ProxyDemoteDirect) {
  bool has_direct = true;
  EXPECT_EQ("", download_mgr.DemoteDirect("", &has_direct));
  EXPECT_FALSE(has_direct);

  // DIRECT on its own stays a tier.
  EXPECT_EQ("DIRECT", download_mgr.DemoteDirect("DIRECT", &has_direct));
  EXPECT_TRUE(has_direct);
  EXPECT_EQ("DIRECT", download_mgr.DemoteDirect("DIRECT|DIRECT", &has_direct));
  EXPECT_TRUE(has_direct);

  // Sharing a load-balance group with a real proxy is not allowed: DIRECT
  // would take a share of the traffic while the proxy is healthy.
  EXPECT_EQ("A", download_mgr.DemoteDirect("A|DIRECT", &has_direct));
  EXPECT_FALSE(has_direct);
  EXPECT_EQ("A|B", download_mgr.DemoteDirect("A|DIRECT|B", &has_direct));
  EXPECT_FALSE(has_direct);

  // A group of its own is kept, and stays in the position it was written in.
  EXPECT_EQ("A;DIRECT", download_mgr.DemoteDirect("A;DIRECT", &has_direct));
  EXPECT_TRUE(has_direct);
  EXPECT_EQ("A;DIRECT;B", download_mgr.DemoteDirect("A;DIRECT;B", &has_direct));
  EXPECT_TRUE(has_direct);
  EXPECT_EQ("A;DIRECT",
            download_mgr.DemoteDirect("A|DIRECT;DIRECT", &has_direct));
  EXPECT_TRUE(has_direct);

  // Chains without DIRECT are passed through untouched.
  EXPECT_EQ("A|B;C", download_mgr.DemoteDirect("A|B;C", &has_direct));
  EXPECT_FALSE(has_direct);
}


TEST_F(T_Download, ProxyChainDefaultsToPreviousBehaviour) {
  // Everything this branch changes about the chain is opt-in.  With
  // CVMFS_PROXY_MANDATORY unset -- the default -- SetProxyChain() must behave
  // exactly as it did before the option existed.
  vector<vector<DownloadManager::ProxyInfo> > chain;
  unsigned current_group = 42;
  unsigned fallback_group = 42;

  // Fallback proxies still discard a configured DIRECT outright.
  download_mgr.SetProxyChain("http://127.0.0.1:3128;DIRECT",
                             "http://127.0.0.2:3128",
                             DownloadManager::kSetProxyBoth);
  download_mgr.GetProxyInfo(&chain, &current_group, &fallback_group);
  ASSERT_EQ(2U, chain.size());
  EXPECT_EQ("http://127.0.0.1:3128", chain[0][0].url);
  EXPECT_EQ("http://127.0.0.2:3128", chain[1][0].url);
  EXPECT_EQ(1U, fallback_group);
  EXPECT_FALSE(download_mgr.opt_proxy_mandatory_);

  // DIRECT is still kept as a peer inside a load-balance group.
  download_mgr.SetProxyChain("http://127.0.0.1:3128|DIRECT", "",
                             DownloadManager::kSetProxyBoth);
  download_mgr.GetProxyInfo(&chain, &current_group, &fallback_group);
  ASSERT_EQ(1U, chain.size());
  EXPECT_EQ(2U, chain[0].size());
  EXPECT_FALSE(download_mgr.opt_proxy_mandatory_);

  // And a proxy-only chain never makes the proxy mandatory by itself.
  download_mgr.SetProxyChain("http://127.0.0.1:3128", "",
                             DownloadManager::kSetProxyBoth);
  EXPECT_FALSE(download_mgr.opt_proxy_mandatory_);
}


TEST_F(T_Download, ProxyDirectKeptAsLastResortTier) {
  vector<vector<DownloadManager::ProxyInfo> > chain;
  unsigned current_group = 42;
  unsigned fallback_group = 42;

  // "proxy;DIRECT" must keep DIRECT as a separate, later tier.
  download_mgr.EnableMandatoryProxy();
  download_mgr.SetProxyChain("http://127.0.0.1:3128;DIRECT", "",
                             DownloadManager::kSetProxyBoth);
  download_mgr.GetProxyInfo(&chain, &current_group, &fallback_group);
  ASSERT_EQ(2U, chain.size());
  ASSERT_EQ(1U, chain[0].size());
  EXPECT_EQ("http://127.0.0.1:3128", chain[0][0].url);
  ASSERT_EQ(1U, chain[1].size());
  EXPECT_EQ("DIRECT", chain[1][0].url);
  // The proxy is where we start, and direct connections are permitted only
  // because the administrator asked for them.
  EXPECT_EQ(0U, current_group);
  EXPECT_FALSE(download_mgr.opt_proxy_mandatory_);

  // Configured fallback proxies must no longer discard the DIRECT tier.  This
  // is the EGI case: the configuration repository always sets fallback
  // proxies, so ";DIRECT" used to be silently inert.
  download_mgr.SetProxyChain("http://127.0.0.1:3128;DIRECT",
                             "http://127.0.0.2:3128",
                             DownloadManager::kSetProxyBoth);
  download_mgr.GetProxyInfo(&chain, &current_group, &fallback_group);
  ASSERT_EQ(3U, chain.size());
  EXPECT_EQ("http://127.0.0.1:3128", chain[0][0].url);
  EXPECT_EQ("DIRECT", chain[1][0].url);
  EXPECT_EQ("http://127.0.0.2:3128", chain[2][0].url);
  EXPECT_EQ(2U, fallback_group);
}


TEST_F(T_Download, ProxyDirectNotPeerOfLiveProxy) {
  vector<vector<DownloadManager::ProxyInfo> > chain;
  unsigned current_group = 42;
  unsigned fallback_group = 42;

  // "proxy|DIRECT" would otherwise load-balance unproxied traffic against a
  // healthy proxy.
  download_mgr.EnableMandatoryProxy();
  download_mgr.SetProxyChain("http://127.0.0.1:3128|DIRECT", "",
                             DownloadManager::kSetProxyBoth);
  download_mgr.GetProxyInfo(&chain, &current_group, &fallback_group);
  ASSERT_EQ(1U, chain.size());
  ASSERT_EQ(1U, chain[0].size());
  EXPECT_EQ("http://127.0.0.1:3128", chain[0][0].url);
  EXPECT_TRUE(download_mgr.opt_proxy_mandatory_);
}


TEST_F(T_Download, ProxyDirectOnlyPreserved) {
  vector<vector<DownloadManager::ProxyInfo> > chain;
  unsigned current_group = 42;
  unsigned fallback_group = 42;

  // Nothing but DIRECT is a deliberate choice: leave unproxied setups alone.
  download_mgr.EnableMandatoryProxy();
  download_mgr.SetProxyChain("DIRECT", "", DownloadManager::kSetProxyBoth);
  download_mgr.GetProxyInfo(&chain, &current_group, &fallback_group);
  ASSERT_EQ(1U, chain.size());
  ASSERT_EQ(1U, chain[0].size());
  EXPECT_EQ("DIRECT", chain[0][0].url);
  EXPECT_FALSE(download_mgr.opt_proxy_mandatory_);
}


TEST_F(T_Download, ProxyMandatorySurvivesClone) {
  // The external download manager is created as a clone, so a proxy policy
  // that does not propagate would apply to catalogs but not to external data.
  download_mgr.EnableMandatoryProxy();
  download_mgr.SetProxyChain("http://127.0.0.1:3128", "",
                             DownloadManager::kSetProxyBoth);
  EXPECT_TRUE(download_mgr.opt_proxy_mandatory_);

  DownloadManager *clone = download_mgr.Clone(
      perf::StatisticsTemplate("clone", &statistics), "clone");
  EXPECT_TRUE(clone->opt_proxy_mandatory_);

  // And a DIRECT-only chain must clone as "direct is allowed".
  download_mgr.SetProxyChain("DIRECT", "", DownloadManager::kSetProxyBoth);
  DownloadManager *clone2 = download_mgr.Clone(
      perf::StatisticsTemplate("clone2", &statistics), "clone2");
  EXPECT_FALSE(clone2->opt_proxy_mandatory_);
  delete clone;
  delete clone2;
}


TEST_F(T_Download, NoProxyChainIsNotSelectable) {
  // The selection helpers must cope with a manager that has no usable proxy
  // rather than dereferencing an empty map or an empty group.
  download_mgr.SetProxyChain("", "", DownloadManager::kSetProxyBoth);
  EXPECT_TRUE(download_mgr.ChooseProxyUnlocked(NULL) == NULL);
  EXPECT_FALSE(download_mgr.opt_proxy_mandatory_);
  // Rebalancing an empty chain must be a no-op, not a crash.
  download_mgr.RebalanceProxies();
  download_mgr.SwitchProxyGroup();
  EXPECT_TRUE(download_mgr.ChooseProxyUnlocked(NULL) == NULL);

  // And once a chain is installed again, selection works.
  download_mgr.SetProxyChain("http://127.0.0.1:3128", "",
                             DownloadManager::kSetProxyBoth);
  EXPECT_TRUE(download_mgr.ChooseProxyUnlocked(NULL) != NULL);
}


TEST_F(T_Download, EscalatedProxyGroupClassification) {
  vector<vector<DownloadManager::ProxyInfo> > chain;
  unsigned current_group = 0;
  unsigned fallback_group = 0;

  // "proxy;DIRECT" plus a fallback: group 0 is local, group 1 is the DIRECT
  // tier, group 2 is off-site.  Only the first is not an escalation.
  download_mgr.EnableMandatoryProxy();
  download_mgr.SetProxyChain("http://127.0.0.1:3128;DIRECT",
                             "http://127.0.0.2:3128",
                             DownloadManager::kSetProxyBoth);
  download_mgr.GetProxyInfo(&chain, &current_group, &fallback_group);
  ASSERT_EQ(3U, chain.size());
  EXPECT_FALSE(download_mgr.IsEscalatedProxyGroup(0));
  EXPECT_TRUE(download_mgr.IsEscalatedProxyGroup(1));    // DIRECT tier
  EXPECT_TRUE(download_mgr.IsEscalatedProxyGroup(2));    // fallback proxy
  EXPECT_FALSE(download_mgr.IsEscalatedProxyGroup(99));  // out of range

  // Two local groups and no DIRECT: moving between them is plain failover.
  download_mgr.SetProxyChain("http://127.0.0.1:3128;http://127.0.0.3:3128", "",
                             DownloadManager::kSetProxyBoth);
  EXPECT_FALSE(download_mgr.IsEscalatedProxyGroup(0));
  EXPECT_FALSE(download_mgr.IsEscalatedProxyGroup(1));
}


// ---------------------------------------------------------------------------
// End-to-end: which peer actually receives the request.  Each of these was
// confirmed to fail before the corresponding fix.
// ---------------------------------------------------------------------------

TEST_F(T_Download, ProxyHealthyProxyIsUsedNotDirect) {
  // With "proxy;DIRECT" and a healthy proxy, every request must go through
  // the proxy; the DIRECT tier must stay untouched.
  string src_path = GetSmallFile();
  MockFileServer file_server(8094, sandbox_path_);
  MockProxyServer proxy_server(8095);
  download_mgr.SetProxyChain("http://127.0.0.1:8095;DIRECT", "",
                             DownloadManager::kSetProxyBoth);

  string src_url = "http://127.0.0.1:8094/" + GetFileName(src_path);
  cvmfs::MemSink memsink;
  JobInfo info(&src_url, false /* compressed */, false /* probe hosts */, NULL,
               &memsink);
  download_mgr.Fetch(&info);

  EXPECT_EQ(kFailOk, info.error_code());
  EXPECT_EQ(1, proxy_server.num_processed_requests());
  EXPECT_EQ("http://127.0.0.1:8095", info.proxy());
}


TEST_F(T_Download, UnresponsiveProxyEscalatesToDirect) {
  // A proxy that never answers the connect is genuinely unreachable, which is
  // what the DIRECT tier exists for.  192.0.2.0/24 is TEST-NET-1 from RFC
  // 5737: it is not routed, so the connect runs out of time rather than being
  // refused, and the connect time stays zero.
  string src_path = GetSmallFile();
  MockFileServer file_server(8096, sandbox_path_);

  download_mgr.SetProxyChain("http://192.0.2.1:3128;DIRECT", "",
                             DownloadManager::kSetProxyBoth);
  download_mgr.SetRetryParameters(1, 0, 0);
  download_mgr.SetTimeout(2, 2);

  string src_url = "http://127.0.0.1:8096/" + GetFileName(src_path);
  cvmfs::MemSink memsink;
  JobInfo info(&src_url, false /* compressed */, false /* probe hosts */, NULL,
               &memsink);
  download_mgr.Fetch(&info);

  EXPECT_EQ(kFailOk, info.error_code());
  EXPECT_EQ("DIRECT", info.proxy());
  EXPECT_EQ(1, file_server.num_processed_requests());
}


TEST_F(T_Download, RefusedProxyDoesNotEscalateToDirect) {
  // The converse, and the case libcurl makes easy to get wrong: a refused
  // connect is reported as CURLE_COULDNT_CONNECT while an unanswered one is
  // CURLE_OPERATION_TIMEDOUT, so keying on the error code alone treats a
  // proxy that is merely out of slots as though it were dead.  Nothing
  // listens on 8097, so the kernel refuses at once -- proof that the host is
  // up -- and the DIRECT tier must stay unused.
  string src_path = GetSmallFile();
  MockFileServer file_server(8098, sandbox_path_);

  download_mgr.SetProxyFailoverOnSlow(false);
  download_mgr.SetProxyChain("http://127.0.0.1:8097;DIRECT", "",
                             DownloadManager::kSetProxyBoth);
  download_mgr.SetRetryParameters(1, 0, 0);
  download_mgr.SetTimeout(2, 2);

  string src_url = "http://127.0.0.1:8098/" + GetFileName(src_path);
  cvmfs::MemSink memsink;
  JobInfo info(&src_url, false /* compressed */, false /* probe hosts */, NULL,
               &memsink);
  download_mgr.Fetch(&info);

  EXPECT_EQ(kFailProxyConnection, info.error_code());
  EXPECT_FALSE(info.peer_unresponsive());
  EXPECT_NE("DIRECT", info.proxy());
  EXPECT_EQ(0, file_server.num_processed_requests());
}


TEST_F(T_Download, ProxyWithoutDirectNeverLeaks) {
  // No DIRECT anywhere in the chain: a dead proxy must fail the request, not
  // silently bypass the proxy.
  string src_path = GetSmallFile();
  MockFileServer file_server(8100, sandbox_path_);

  // Nothing listens on 8099 and no DIRECT tier is offered.
  download_mgr.EnableMandatoryProxy();
  download_mgr.SetProxyChain("http://127.0.0.1:8099", "",
                             DownloadManager::kSetProxyBoth);

  string src_url = "http://127.0.0.1:8100/" + GetFileName(src_path);
  cvmfs::MemSink memsink;
  JobInfo info(&src_url, false /* compressed */, false /* probe hosts */, NULL,
               &memsink);
  download_mgr.Fetch(&info);

  EXPECT_NE(kFailOk, info.error_code());
  EXPECT_EQ(0, file_server.num_processed_requests());

  // Positive control: the origin was reachable all along.
  download_mgr.SetProxyChain("DIRECT", "", DownloadManager::kSetProxyBoth);
  memsink.Reset();
  JobInfo info_direct(&src_url, false /* compressed */, false /* probe hosts */,
                      NULL, &memsink);
  download_mgr.Fetch(&info_direct);
  EXPECT_EQ(kFailOk, info_direct.error_code());
  EXPECT_EQ(1, file_server.num_processed_requests());
}


namespace {
// Stands in for a saturated proxy: it answers, so it is plainly reachable,
// but the body is cut short.  That is a throughput symptom, reported as
// kFailProxyShortTransfer, not evidence of a dead peer.
HTTPResponse TruncatingProxyHandler(const HTTPRequest & /* req */, void *data) {
  int *hits = static_cast<int *>(data);
  if (hits)
    (*hits)++;
  HTTPResponse response;
  response.raw = true;
  response.body = "HTTP/1.1 200 OK\r\nContent-Length: 65536\r\n\r\nshort";
  return response;
}

HTTPResponse CountingProxyHandler(const HTTPRequest & /* req */, void *data) {
  int *hits = static_cast<int *>(data);
  if (hits)
    (*hits)++;
  HTTPResponse response;
  response.code = 200;
  response.body = "served by the fallback proxy";
  return response;
}
}  // namespace


TEST_F(T_Download, SlowProxyDoesNotEscalateToDirect) {
  // The regression for the leak measured under load: a proxy that is merely
  // saturated must not promote the DIRECT tier, because that puts unproxied
  // traffic on the wire and then lets host failover roam the server list.
  string src_path = GetSmallFile();
  MockFileServer origin(8110, sandbox_path_);
  int proxy_hits = 0;
  MockHTTPServer slow_proxy(8111);
  ASSERT_TRUE(
      slow_proxy.SetResponseCallback(TruncatingProxyHandler, &proxy_hits));
  ASSERT_TRUE(slow_proxy.Start());

  download_mgr.SetProxyFailoverOnSlow(false);
  download_mgr.SetProxyChain("http://127.0.0.1:8111;DIRECT", "",
                             DownloadManager::kSetProxyBoth);
  download_mgr.SetRetryParameters(2, 0, 0);

  const string url = "http://127.0.0.1:8110/" + GetFileName(src_path);
  cvmfs::MemSink sink;
  JobInfo info(&url, false /* compressed */, false /* probe hosts */, NULL,
               &sink);
  download_mgr.Fetch(&info);

  EXPECT_NE(kFailOk, info.error_code());
  EXPECT_GT(proxy_hits, 0) << "the proxy was never even tried";
  // The decisive assertion: the origin was never contacted directly.
  EXPECT_EQ(0, origin.num_processed_requests());
  EXPECT_NE("DIRECT", info.proxy());
  slow_proxy.Stop();
}


namespace {
/**
 * A socket that is bound and listening but never accepted from.  The kernel
 * completes the TCP handshake for it, so a connect succeeds, and the request
 * that follows is then never answered.  That is what a dead proxy looks like
 * from the client: the listening socket outlives whatever was serving it.
 *
 * Start() reports failure rather than asserting, so the test still fails
 * loudly in a build with NDEBUG defined.
 */
class SilentListener {
 public:
  SilentListener() : fd_(-1) { }
  ~SilentListener() {
    if (fd_ >= 0)
      close(fd_);
  }

  bool Start(int port) {
    fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0)
      return false;
    const int on = 1;
    setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    // ::bind, not std::bind -- this file has "using namespace std".
    if (::bind(fd_, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr))
        != 0) {
      return false;
    }
    return listen(fd_, 16) == 0;
  }

 private:
  int fd_;
};
}  // namespace


TEST_F(T_Download, SilentProxyCountsAsUnreachable) {
  // A proxy that accepts the connection and then sends nothing is dead, not
  // slow, and must still be escalated away from even with the escalation gate
  // in force.  Keying the verdict on the connect alone got this wrong: the
  // handshake completes, so the peer looked alive and the client stayed on a
  // proxy that would never answer.
  string src_path = GetSmallFile();
  MockFileServer origin(8119, sandbox_path_);
  SilentListener silent_proxy;
  ASSERT_TRUE(silent_proxy.Start(8118));

  download_mgr.SetProxyFailoverOnSlow(false);
  download_mgr.SetProxyChain("http://127.0.0.1:8118;DIRECT", "",
                             DownloadManager::kSetProxyBoth);
  download_mgr.SetRetryParameters(1, 0, 0);
  download_mgr.SetTimeout(2, 2);

  const string url = "http://127.0.0.1:8119/" + GetFileName(src_path);
  cvmfs::MemSink sink;
  JobInfo info(&url, false /* compressed */, false /* probe hosts */, NULL,
               &sink);
  download_mgr.Fetch(&info);

  EXPECT_EQ(kFailOk, info.error_code());
  EXPECT_EQ("DIRECT", info.proxy());
  EXPECT_EQ(1, origin.num_processed_requests());
}


TEST_F(T_Download, SlowProxyEscalatesToDirectByDefault) {
  // The mirror image of the test above, and the one that shows the gate is
  // genuinely opt-in: with CVMFS_PROXY_FAILOVER_ON_SLOW left at its default a
  // saturated proxy still promotes the DIRECT tier and the origin is still
  // contacted unproxied, exactly as before this branch.
  string src_path = GetSmallFile();
  MockFileServer origin(8116, sandbox_path_);
  int proxy_hits = 0;
  MockHTTPServer slow_proxy(8117);
  ASSERT_TRUE(
      slow_proxy.SetResponseCallback(TruncatingProxyHandler, &proxy_hits));
  ASSERT_TRUE(slow_proxy.Start());

  download_mgr.SetProxyChain("http://127.0.0.1:8117;DIRECT", "",
                             DownloadManager::kSetProxyBoth);
  download_mgr.SetRetryParameters(2, 0, 0);

  const string url = "http://127.0.0.1:8116/" + GetFileName(src_path);
  cvmfs::MemSink sink;
  JobInfo info(&url, false /* compressed */, false /* probe hosts */, NULL,
               &sink);
  download_mgr.Fetch(&info);

  EXPECT_GT(proxy_hits, 0) << "the proxy was never even tried";
  EXPECT_EQ("DIRECT", info.proxy());
  EXPECT_GT(origin.num_processed_requests(), 0);
  slow_proxy.Stop();
}


TEST_F(T_Download, SlowProxyDoesNotEscalateToFallback) {
  // Same rule for the off-site fallback proxies: saturation at the local
  // proxy is not a reason to ship the request to another site.
  string src_path = GetSmallFile();
  MockFileServer origin(8112, sandbox_path_);
  int slow_hits = 0;
  int fallback_hits = 0;
  MockHTTPServer slow_proxy(8113);
  ASSERT_TRUE(
      slow_proxy.SetResponseCallback(TruncatingProxyHandler, &slow_hits));
  ASSERT_TRUE(slow_proxy.Start());
  MockHTTPServer fallback_proxy(8114);
  ASSERT_TRUE(
      fallback_proxy.SetResponseCallback(CountingProxyHandler, &fallback_hits));
  ASSERT_TRUE(fallback_proxy.Start());

  download_mgr.SetProxyFailoverOnSlow(false);
  download_mgr.SetProxyChain("http://127.0.0.1:8113",
                             "http://127.0.0.1:8114",
                             DownloadManager::kSetProxyBoth);
  download_mgr.SetRetryParameters(2, 0, 0);

  const string url = "http://127.0.0.1:8112/" + GetFileName(src_path);
  cvmfs::MemSink sink;
  JobInfo info(&url, false /* compressed */, false /* probe hosts */, NULL,
               &sink);
  download_mgr.Fetch(&info);

  EXPECT_NE(kFailOk, info.error_code());
  EXPECT_GT(slow_hits, 0);
  EXPECT_EQ(0, fallback_hits) << "a saturated local proxy escalated off-site";
  EXPECT_EQ(0, origin.num_processed_requests());
  slow_proxy.Stop();
  fallback_proxy.Stop();
}


TEST_F(T_Download, ValidateGeoReply) {
  vector<uint64_t> geo_order;
  EXPECT_FALSE(download_mgr.ValidateGeoReply("", geo_order.size(), &geo_order));

  geo_order.push_back(0);
  EXPECT_FALSE(
      download_mgr.ValidateGeoReply("a", geo_order.size(), &geo_order));
  EXPECT_FALSE(
      download_mgr.ValidateGeoReply("1,1", geo_order.size(), &geo_order));
  EXPECT_FALSE(
      download_mgr.ValidateGeoReply("1,3", geo_order.size(), &geo_order));
  EXPECT_FALSE(
      download_mgr.ValidateGeoReply("2,3", geo_order.size(), &geo_order));
  EXPECT_FALSE(
      download_mgr.ValidateGeoReply("2", geo_order.size(), &geo_order));
  EXPECT_TRUE(download_mgr.ValidateGeoReply("1", geo_order.size(), &geo_order));
  EXPECT_EQ(geo_order.size(), 1U);
  EXPECT_EQ(geo_order[0], 0U);

  geo_order.push_back(0);
  EXPECT_FALSE(
      download_mgr.ValidateGeoReply(",", geo_order.size(), &geo_order));
  EXPECT_FALSE(
      download_mgr.ValidateGeoReply("2,", geo_order.size(), &geo_order));
  EXPECT_FALSE(
      download_mgr.ValidateGeoReply("1", geo_order.size(), &geo_order));
  EXPECT_FALSE(
      download_mgr.ValidateGeoReply("3,2,1", geo_order.size(), &geo_order));
  EXPECT_TRUE(
      download_mgr.ValidateGeoReply("2,1", geo_order.size(), &geo_order));
  EXPECT_EQ(geo_order.size(), 2U);
  EXPECT_EQ(geo_order[0], 1U);
  EXPECT_EQ(geo_order[1], 0U);

  EXPECT_TRUE(
      download_mgr.ValidateGeoReply("2,1\n", geo_order.size(), &geo_order));
  EXPECT_EQ(geo_order.size(), 2U);
  EXPECT_EQ(geo_order[0], 1U);
  EXPECT_EQ(geo_order[1], 0U);

  geo_order.push_back(0);
  geo_order.push_back(0);
  EXPECT_TRUE(
      download_mgr.ValidateGeoReply("4,3,1,2\n", geo_order.size(), &geo_order));
  EXPECT_EQ(geo_order.size(), 4U);
  EXPECT_EQ(geo_order[0], 3U);
  EXPECT_EQ(geo_order[1], 2U);
  EXPECT_EQ(geo_order[2], 0U);
  EXPECT_EQ(geo_order[3], 1U);
}


TEST_F(T_Download, ParseHttpCode) {
  char digits[3];
  digits[0] = '0';
  digits[1] = '0';
  digits[2] = 'a';
  EXPECT_EQ(-1, DownloadManager::ParseHttpCode(digits));
  digits[0] = '0';
  digits[1] = '0';
  digits[2] = '0';
  EXPECT_EQ(0, DownloadManager::ParseHttpCode(digits));
  digits[0] = '0';
  digits[1] = '0';
  digits[2] = '1';
  EXPECT_EQ(1, DownloadManager::ParseHttpCode(digits));
  digits[0] = '1';
  digits[1] = '0';
  digits[2] = '1';
  EXPECT_EQ(101, DownloadManager::ParseHttpCode(digits));
  digits[0] = '9';
  digits[1] = '9';
  digits[2] = '9';
  EXPECT_EQ(999, DownloadManager::ParseHttpCode(digits));
}

TEST_F(T_Download, EscapeUrl) {
  const std::string url = "http://ab0341.¡ÿϦ랝";  // c2a1 c3bf cfa6 eb9e9d
  const std::string correct = "http://ab0341.%C2%A1%C3%BF%CF%A6%EB%9E%9D";
  const std::string res = download_mgr.EscapeUrl(0, url);

  EXPECT_TRUE(res == correct);
}

}  // namespace download
