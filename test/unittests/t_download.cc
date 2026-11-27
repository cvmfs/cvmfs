/**
 * This file is part of the CernVM File System.
 */

#include <fcntl.h>
#include <pthread.h>
#include <sys/resource.h>
#include <sys/select.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdio>
#include <vector>

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
#include "util/platform.h"
#include "util/posix.h"
#include "util/prng.h"

using namespace std;  // NOLINT

namespace {

class TestInterruptCue : public InterruptCue {
 public:
  virtual bool IsCanceled() { return true; }
};

struct FlakyServerState {
  FlakyServerState(int code, int num_failures)
      : code(code), num_failures(num_failures), num_requests(0) { }
  int code;
  int num_failures;
  int num_requests;
};

HTTPResponse FlakyServerHandler(const HTTPRequest & /*req*/, void *data) {
  FlakyServerState *state = static_cast<FlakyServerState *>(data);
  HTTPResponse response;
  if (state->num_requests++ < state->num_failures) {
    response.code = state->code;
    response.reason = "Error";
  } else {
    response.body = "payload";
  }
  return response;
}

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


TEST_F(T_Download, RemoteFileHighFd) {
  // Without a working poll(), libcurl falls back to select() and rejects
  // sockets >= FD_SETSIZE, which shows up as kFailHostConnection.
  const int kNumFds = FD_SETSIZE + 64;
  struct rlimit rl;
  ASSERT_EQ(0, getrlimit(RLIMIT_NOFILE, &rl));
  const rlim_t orig_soft = rl.rlim_cur;
  if (rl.rlim_cur < static_cast<rlim_t>(2 * kNumFds)) {
    if (rl.rlim_max < static_cast<rlim_t>(2 * kNumFds))
      GTEST_SKIP() << "RLIMIT_NOFILE hard limit too low";
    rl.rlim_cur = 2 * kNumFds;
    ASSERT_EQ(0, setrlimit(RLIMIT_NOFILE, &rl));
  }

  MockFileServer file_server(8082, sandbox_path_);
  const string src_path = GetSmallFile();
  string url = "http://127.0.0.1:8082/" + GetFileName(src_path);

  std::vector<int> fds;
  const int fd_null = open("/dev/null", O_RDONLY);
  ASSERT_GE(fd_null, 0);
  fds.push_back(fd_null);
  while (fds.back() < kNumFds) {
    const int fd = dup(fd_null);
    ASSERT_GE(fd, 0);
    fds.push_back(fd);
  }

  cvmfs::MemSink memsink;
  JobInfo info(&url, false, false, NULL, &memsink);
  download_mgr.Fetch(&info);

  for (unsigned i = 0; i < fds.size(); ++i)
    close(fds[i]);
  rl.rlim_cur = orig_soft;
  setrlimit(RLIMIT_NOFILE, &rl);

  EXPECT_EQ(kFailOk, info.error_code());
  EXPECT_EQ(1, file_server.num_processed_requests());
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

TEST_F(T_Download, RemoteFileRetryGatewayError) {
  const int codes[] = {502, 503, 504};
  for (unsigned i = 0; i < sizeof(codes) / sizeof(codes[0]); ++i) {
    FlakyServerState state(codes[i], 1);
    const int port = 8085 + i;
    MockHTTPServer server(port);
    server.SetResponseCallback(FlakyServerHandler, &state);
    ASSERT_TRUE(server.Start());

    download_mgr.SetRetryParameters(1, 1, 2);
    string url = "http://127.0.0.1:" + StringifyInt(port) + "/foo";
    cvmfs::MemSink memsink;
    JobInfo info(&url, false /* compressed */, false /* probe hosts */, NULL,
                 &memsink);
    download_mgr.Fetch(&info);
    EXPECT_EQ(kFailOk, info.error_code()) << codes[i];
    EXPECT_EQ(2, state.num_requests) << codes[i];
    EXPECT_EQ(1U, info.num_retries()) << codes[i];
  }
}

TEST_F(T_Download, RemoteFileGatewayErrorExhaustsRetries) {
  FlakyServerState state(502, 100);
  MockHTTPServer server(8085);
  server.SetResponseCallback(FlakyServerHandler, &state);
  ASSERT_TRUE(server.Start());

  download_mgr.SetRetryParameters(2, 1, 2);
  string url = "http://127.0.0.1:8085/foo";
  cvmfs::MemSink memsink;
  JobInfo info(&url, false /* compressed */, false /* probe hosts */, NULL,
               &memsink);
  download_mgr.Fetch(&info);
  EXPECT_EQ(kFailHostConnection, info.error_code());
  EXPECT_EQ(502, info.http_code());
  EXPECT_EQ(3, state.num_requests);
}

TEST_F(T_Download, RemoteFileNoRetryInternalServerError) {
  FlakyServerState state(500, 1);
  MockHTTPServer server(8085);
  server.SetResponseCallback(FlakyServerHandler, &state);
  ASSERT_TRUE(server.Start());

  download_mgr.SetRetryParameters(1, 1, 2);
  string url = "http://127.0.0.1:8085/foo";
  cvmfs::MemSink memsink;
  JobInfo info(&url, false /* compressed */, false /* probe hosts */, NULL,
               &memsink);
  download_mgr.Fetch(&info);
  EXPECT_EQ(kFailHostHttp, info.error_code());
  EXPECT_EQ(1, state.num_requests);
}

namespace {

struct BackoffFetchArgs {
  DownloadManager *download_mgr;
  string url;
  Failures result;
  unsigned num_retries;
};

void *BackoffFetchThread(void *data) {
  BackoffFetchArgs *args = static_cast<BackoffFetchArgs *>(data);
  cvmfs::MemSink memsink;
  JobInfo info(&args->url, false /* compressed */, false /* probe hosts */,
               NULL, &memsink);
  args->result = args->download_mgr->Fetch(&info);
  args->num_retries = info.num_retries();
  return NULL;
}

// Reports the cancellation only after the first num_not_canceled queries
class CancelLaterInterruptCue : public InterruptCue {
 public:
  explicit CancelLaterInterruptCue(int num_not_canceled)
      : num_not_canceled_(num_not_canceled), num_queries_(0) { }
  virtual bool IsCanceled() { return num_queries_++ >= num_not_canceled_; }

 private:
  const int num_not_canceled_;
  std::atomic<int> num_queries_;
};

struct CountingErrorState {
  CountingErrorState() : num_requests(0) { }
  std::atomic<int> num_requests;
};

HTTPResponse AlwaysUnavailableHandler(const HTTPRequest & /*req*/,
                                      void *data) {
  static_cast<CountingErrorState *>(data)->num_requests++;
  HTTPResponse response;
  response.code = 503;
  response.reason = "Service Unavailable";
  return response;
}

}  // anonymous namespace

// In multi-threaded mode, a retry after backoff must still be carried out
// (the parked curl handle is re-added to the multi handle).
TEST_F(T_Download, MultiThreadedRetryGatewayError) {
  FlakyServerState state(503, 1);
  MockHTTPServer server(8095);
  server.SetResponseCallback(FlakyServerHandler, &state);
  ASSERT_TRUE(server.Start());

  perf::Statistics mt_statistics;
  DownloadManager mt_download_mgr(
      8, perf::StatisticsTemplate("test_mt", &mt_statistics));
  mt_download_mgr.SetRetryParameters(1, 200, 200);
  mt_download_mgr.Spawn();

  string url = "http://127.0.0.1:8095/foo";
  cvmfs::MemSink memsink;
  JobInfo info(&url, false /* compressed */, false /* probe hosts */, NULL,
               &memsink);
  const uint64_t t0 = platform_monotonic_time_ns();
  mt_download_mgr.Fetch(&info);
  const uint64_t elapsed_ms = (platform_monotonic_time_ns() - t0) / 1000000;
  EXPECT_EQ(kFailOk, info.error_code());
  EXPECT_EQ(2, state.num_requests);
  EXPECT_EQ(1U, info.num_retries());
  // The retry waits for the backoff (between 1 and 200 ms)
  EXPECT_GE(elapsed_ms, info.backoff_ms());
}

// A job that is canceled while it waits for its backoff to expire is given up
// with kFailCanceled; the download manager stays usable afterwards.
TEST_F(T_Download, MultiThreadedCancelDuringBackoff) {
  FlakyServerState state(503, 1);
  MockHTTPServer server(8098);
  server.SetResponseCallback(FlakyServerHandler, &state);
  ASSERT_TRUE(server.Start());

  perf::Statistics mt_statistics;
  DownloadManager mt_download_mgr(
      8, perf::StatisticsTemplate("test_mt", &mt_statistics));
  mt_download_mgr.SetRetryParameters(3, 50, 50);
  mt_download_mgr.Spawn();

  string url = "http://127.0.0.1:8098/foo";
  {
    cvmfs::MemSink memsink;
    JobInfo info(&url, false /* compressed */, false /* probe hosts */, NULL,
                 &memsink);
    // Not yet canceled when the retry is decided, canceled when the backoff
    // expired
    CancelLaterInterruptCue cue(1);
    info.SetInterruptCue(&cue);
    mt_download_mgr.Fetch(&info);
    EXPECT_EQ(kFailCanceled, info.error_code());
    EXPECT_EQ(1, state.num_requests);
    EXPECT_EQ(1U, info.num_retries());
    EXPECT_EQ(NULL, info.headers());
  }

  cvmfs::MemSink memsink;
  JobInfo info(&url, false /* compressed */, false /* probe hosts */, NULL,
               &memsink);
  mt_download_mgr.Fetch(&info);
  EXPECT_EQ(kFailOk, info.error_code());
  EXPECT_EQ(2, state.num_requests);
}

// Regression test for issue #3563: a job that backs off must not stall the
// other transfers of the same (multi-threaded) download manager.
TEST_F(T_Download, MultiThreadedBackoffDoesNotStall) {
  CountingErrorState bad_state;
  MockHTTPServer bad_server(8096);
  bad_server.SetResponseCallback(AlwaysUnavailableHandler, &bad_state);
  ASSERT_TRUE(bad_server.Start());

  const string src_path = GetSmallFile();
  MockFileServer file_server(8097, sandbox_path_);
  const string good_url = "http://127.0.0.1:8097/" + GetFileName(src_path);

  perf::Statistics mt_statistics;
  DownloadManager mt_download_mgr(
      8, perf::StatisticsTemplate("test_mt", &mt_statistics));
  const unsigned kMaxRetries = 3;
  const unsigned kBackoffMs = 1000;
  mt_download_mgr.SetRetryParameters(kMaxRetries, kBackoffMs, kBackoffMs);
  mt_download_mgr.Spawn();

  BackoffFetchArgs bad_args;
  bad_args.download_mgr = &mt_download_mgr;
  bad_args.url = "http://127.0.0.1:8096/foo";
  bad_args.result = kFailOk;
  bad_args.num_retries = 0;
  pthread_t bad_thread;
  ASSERT_EQ(0, pthread_create(&bad_thread, NULL, BackoffFetchThread,
                              &bad_args));

  // Wait until the failing job entered its retry loop
  const uint64_t wait_start = platform_monotonic_time_ns();
  while (bad_state.num_requests.load() < 2) {
    ASSERT_LT(platform_monotonic_time_ns() - wait_start, 10000000000ULL);
    SafeSleepMs(10);
  }

  // Fetch healthy files while the other job keeps backing off. Before the fix,
  // each of these blocked for up to kBackoffMs.
  uint64_t max_latency_ms = 0;
  unsigned num_fetches = 0;
  for (unsigned i = 0; i < 15; ++i) {
    cvmfs::MemSink memsink;
    JobInfo info(&good_url, false /* compressed */, false /* probe hosts */,
                 NULL, &memsink);
    const uint64_t t0 = platform_monotonic_time_ns();
    mt_download_mgr.Fetch(&info);
    const uint64_t latency_ms = (platform_monotonic_time_ns() - t0) / 1000000;
    EXPECT_EQ(kFailOk, info.error_code());
    max_latency_ms = std::max(max_latency_ms, latency_ms);
    num_fetches++;
    SafeSleepMs(100);
  }

  pthread_join(bad_thread, NULL);
  EXPECT_EQ(kFailHostConnection, bad_args.result);
  EXPECT_EQ(kMaxRetries, bad_args.num_retries);
  EXPECT_EQ(static_cast<int>(kMaxRetries) + 1, bad_state.num_requests.load());
  EXPECT_EQ(15U, num_fetches);
  EXPECT_LT(max_latency_ms, kBackoffMs / 2);
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


TEST_F(T_Download, NoProxyChainIsNotSelectable) {
  download_mgr.SetProxyChain("http://127.0.0.1:3128", "",
                             DownloadManager::kSetProxyBoth);
  ASSERT_TRUE(download_mgr.ChooseProxyUnlocked(NULL) != NULL);

  // Every proxy of the group burned
  download_mgr.opt_proxy_groups_current_burned_ = 1;
  download_mgr.UpdateProxiesUnlocked("test");
  EXPECT_TRUE(download_mgr.ChooseProxyUnlocked(NULL) == NULL);
  EXPECT_TRUE(download_mgr.opt_proxies_.empty());

  // Group emptied, as by a DNS refresh that dropped every address
  download_mgr.opt_proxy_groups_current_burned_ = 0;
  (*download_mgr.opt_proxy_groups_)[0].clear();
  download_mgr.UpdateProxiesUnlocked("test");
  EXPECT_TRUE(download_mgr.ChooseProxyUnlocked(NULL) == NULL);
  EXPECT_TRUE(download_mgr.opt_proxies_.empty());

  download_mgr.SetProxyChain("http://127.0.0.1:3128", "",
                             DownloadManager::kSetProxyBoth);
  EXPECT_TRUE(download_mgr.ChooseProxyUnlocked(NULL) != NULL);
}

}  // namespace download
