/**
 * This file is part of the CernVM File System.
 */

#include <poll.h>
#include <pthread.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <map>
#include <string>
#include <vector>

#include "download.h"
#include "json.h"
#include "letter.h"
#include "logging.h"
#include "mongoose.h"
#include "options.h"
#include "platform.h"
#include "signature.h"
#include "stratum_agent/uri_map.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/string.h"
#include "uuid.h"
#include "whitelist.h"

using namespace std;

/**
 * Everything that's needed to verify requests for a single repository
 */
struct RepositoryConfig : SingleCopy {
  RepositoryConfig()
    : download_mgr(NULL), signature_mgr(NULL), options_mgr(NULL) { }
  ~RepositoryConfig() {
    delete download_mgr;
    delete signature_mgr;
    delete options_mgr;
    delete statistics;
  }
  string alias;
  string fqrn;
  string stratum0_url;
  download::DownloadManager *download_mgr;
  signature::SignatureManager *signature_mgr;
  OptionsManager *options_mgr;
  perf::Statistics *statistics;
};


struct Job : SingleCopy {
  Job() : fd_stdin(-1), fd_stdout(-1), fd_stderr(-1),
          status(kStatusLimbo), exit_code(-1),
          birth(platform_monotonic_time()), death(0), pid(0) { }
  enum Status {
    kStatusLimbo,
    kStatusRunning,
    kStatusDone
  };
  int fd_stdin;
  int fd_stdout;
  int fd_stderr;
  string stdout;
  string stderr;
  Status status;
  int exit_code;
  pthread_t thread_job;
  uint64_t birth;
  uint64_t death;
  pid_t pid;
};

map<string, RepositoryConfig *> g_repositories;
map<string, Job *> g_jobs;
UriMap g_uri_map;


/**
 * Parse /etc/cvmfs/repositories.d/<fqrn>/.conf and search for stratum 1s
 */
static void ReadConfiguration() {
  vector<string> repo_config_dirs =
    FindDirectories("/etc/cvmfs/repositories.d");
  for (unsigned i = 0; i < repo_config_dirs.size(); ++i) {
    string name = GetFileName(repo_config_dirs[i]);
    string optarg;
    UniquePtr<OptionsManager> options_mgr(new BashOptionsManager());
    options_mgr->set_taint_environment(false);
    options_mgr->ParsePath(repo_config_dirs[i] + "/server.conf", false);
    if (!options_mgr->GetValue("CVMFS_REPOSITORY_TYPE", &optarg) ||
        (optarg != "stratum1"))
    {
      continue;
    }
    options_mgr->ParsePath(repo_config_dirs[i] + "/replica.conf", false);

    UniquePtr<signature::SignatureManager>
      signature_mgr(new signature::SignatureManager());
    signature_mgr->Init();
    options_mgr->GetValue("CVMFS_PUBLIC_KEY", &optarg);
    if (!signature_mgr->LoadPublicRsaKeys(optarg)) {
      LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr,
               "(%s) could not load public key %s",
               name.c_str(), optarg.c_str());
      continue;
    }

    UniquePtr<perf::Statistics> statistics(new perf::Statistics());
    UniquePtr<download::DownloadManager>
      download_mgr(new download::DownloadManager());
    download_mgr->Init(4, false, statistics);
    if (options_mgr->GetValue("CVMFS_HTTP_TIMEOUT", &optarg))
      download_mgr->SetTimeout(String2Uint64(optarg), String2Uint64(optarg));
    if (options_mgr->GetValue("CVMFS_HTTP_RETRIES", &optarg))
      download_mgr->SetRetryParameters(String2Uint64(optarg), 1000, 2000);
    RepositoryConfig *config = new RepositoryConfig();
    config->alias = name;
    options_mgr->GetValue("CVMFS_REPOSITORY_NAME", &(config->fqrn));
    options_mgr->GetValue("CVMFS_STRATUM0", &(config->stratum0_url));
    config->signature_mgr = signature_mgr.Release();
    config->download_mgr = download_mgr.Release();
    config->options_mgr = options_mgr.Release();
    config->statistics = statistics.Release();
    g_repositories[name] = config;
    LogCvmfs(kLogCvmfs, kLogStdout | kLogSyslog,
             "watching %s (fqrn: %s)", name.c_str(), config->fqrn.c_str());
  }
  if (g_repositories.empty()) {
    LogCvmfs(kLogCvmfs, kLogStdout | kLogSyslogWarn,
             "Warning: no stratum 1 repositories found");
  }
}


/**
 * New replication job
 */
class UriHandlerReplicate : public UriHandler {
 public:
  explicit UriHandlerReplicate(RepositoryConfig *config) : config_(config) { }
  virtual void OnRequest(const struct mg_request_info *req_info,
                         struct mg_connection *conn)
  {
    char post_data[kMaxPostData];  post_data[0] = '\0';
    unsigned post_data_len = mg_read(conn, post_data, sizeof(post_data) - 1);
    post_data[post_data_len] = '\0';
    // Trim trailing newline
    if (post_data_len && (post_data[post_data_len - 1] == '\n'))
      post_data[post_data_len - 1] = '\0';

    // Verify letter
    string message;
    string cert;
    letter::Letter letter(config_->fqrn, post_data, config_->signature_mgr);
    letter::Failures retval_lt = letter.Verify(kTimeoutLetter, &message, &cert);
    if (retval_lt != letter::kFailOk) {
      WebReply::Send(WebReply::k400, MkJsonError(letter::Code2Ascii(retval_lt)),
                     conn);
      return;
    }
    whitelist::Whitelist whitelist(config_->fqrn, config_->download_mgr,
                                   config_->signature_mgr);
    whitelist::Failures retval_wl = whitelist.Load(config_->stratum0_url);
    if (retval_wl == whitelist::kFailOk)
      retval_wl = whitelist.VerifyLoadedCertificate();
    if (retval_wl != whitelist::kFailOk) {
      WebReply::Send(WebReply::k400,
                     MkJsonError(whitelist::Code2Ascii(retval_wl)), conn);
      return;
    }

    UniquePtr<Job> job(new Job());
    string exe = "/usr/bin/cvmfs_server";
    vector<string> argv;
    argv.push_back("snapshot");
    argv.push_back(config_->alias);
    bool retval_b = ExecuteBinary(
      &job->fd_stdin, &job->fd_stdout, &job->fd_stderr,
      exe, argv, false, &job->pid);
    if (!retval_b) {
      WebReply::Send(WebReply::k500,
                     MkJsonError("could not spawn snapshot process"), conn);
      return;
    }
    int retval_i = pthread_create(&job->thread_job, NULL, MainJobMgr, job);
    if (retval_i == 0) {
      job->status = Job::kStatusRunning;
    } else {
      close(job->fd_stdin);
      close(job->fd_stdout);
      close(job->fd_stderr);
    }
    string uuid = cvmfs::Uuid::CreateOneTime();
    g_jobs[uuid] = job.Release();
    WebReply::Send(WebReply::k200, "{\"job_id\":\"" + uuid + "\"}", conn);
  }

 private:
  static const unsigned kMaxPostData = 32 * 1024;  // 32kB
  static const unsigned kTimeoutLetter = 180;  // 3 minutes

  static void *MainJobMgr(void *data) {
    Job *job = reinterpret_cast<Job *>(data);
    Block2Nonblock(job->fd_stdout);
    Block2Nonblock(job->fd_stderr);
    char buf_stdout[kPageSize];
    char buf_stderr[kPageSize];
    struct pollfd watch_fds[2];
    watch_fds[0].fd = job->fd_stdout;
    watch_fds[1].fd = job->fd_stderr;
    watch_fds[0].events = watch_fds[1].events = POLLIN | POLLPRI | POLLHUP;
    watch_fds[0].revents = watch_fds[1].revents = 0;
    bool terminate = false;
    while (!terminate) {
      int retval = poll(watch_fds, 2, -1);
      if (retval < 0)
        continue;
      if (watch_fds[0].revents) {
        watch_fds[0].revents = 0;
        int nbytes = read(watch_fds[0].fd, buf_stdout, kPageSize);
        if ((nbytes <= 0) && (errno != EINTR))
          terminate = true;
        if (nbytes > 0)
          job->stdout += string(buf_stdout, nbytes);
      }
      if (watch_fds[1].revents) {
        watch_fds[1].revents = 0;
        int nbytes = read(watch_fds[1].fd, buf_stderr, kPageSize);
        if ((nbytes <= 0) && (errno != EINTR))
          terminate = true;
        if (nbytes > 0)
          job->stderr += string(buf_stderr, nbytes);
      }
    }
    close(job->fd_stdin);
    close(job->fd_stdout);
    close(job->fd_stderr);
    job->exit_code = WaitForChild(job->pid);
    job->death = platform_monotonic_time();
    job->status = Job::kStatusDone;
    return NULL;
  }

  string MkJsonError(const string &msg) {
    return "{\"error\": \"" + msg + "\"}";
  }

  RepositoryConfig *config_;
};


class UriHandlerJob : public UriHandler {
 public:
  virtual void OnRequest(const struct mg_request_info *req_info,
                         struct mg_connection *conn)
  {
    string what = GetFileName(req_info->uri);
    string job_id = GetFileName(GetParentPath(req_info->uri));
    map<string, Job *>::const_iterator iter = g_jobs.find(job_id);
    if (iter == g_jobs.end()) {
      WebReply::Send(WebReply::k404, "{\"error\":\"no such job\"}", conn);
      return;
    }
    Job *job = iter->second;
    if (what == "stdout") {
      WebReply::Send(WebReply::k200, job->stdout, conn);
    } else if (what == "stderr") {
      WebReply::Send(WebReply::k200, job->stderr, conn);
    } else if (what == "status") {
      string reply = "{\"status\":";
      switch (job->status) {
        case Job::kStatusLimbo: reply += "\"limbo\""; break;
        case Job::kStatusRunning: reply += "\"running\""; break;
        case Job::kStatusDone: reply += "\"done\""; break;
        default: assert(false);
      }
      if (job->status == Job::kStatusDone) {
        reply += ",\"exit_code\":" + StringifyInt(job->exit_code);
        reply += ",\"duration\":" + StringifyInt(job->death - job->birth);
      }
      reply += "}";
      WebReply::Send(WebReply::k200, reply, conn);
    } else {
      WebReply::Send(WebReply::k404, "{\"error\":\"internal error\"}", conn);
    }
  }
};


// This function will be called by mongoose on every new request.
static int begin_request_handler(struct mg_connection *conn) {
  const struct mg_request_info *request_info = mg_get_request_info(conn);
  WebRequest request(request_info);
  UriHandler *handler = g_uri_map.Route(request);
  if (handler == NULL) {
    if (g_uri_map.IsKnownUri(request.uri()))
      WebReply::Send(WebReply::k405, "", conn);
    else
      WebReply::Send(WebReply::k404, "", conn);
    return 1;
  }

  handler->OnRequest(request_info, conn);
  return 1;
}


int main(int argc, char **argv) {
  ReadConfiguration();
  for (map<string, RepositoryConfig *>::const_iterator i =
       g_repositories.begin(), i_end = g_repositories.end(); i != i_end; ++i)
  {
    string fqrn = i->second->fqrn;
    g_uri_map.Register(WebRequest(
      "/cvmfs/" + fqrn + "/api/v1/replicate/*/stdout", WebRequest::kGet),
      new UriHandlerJob());
    g_uri_map.Register(WebRequest(
      "/cvmfs/" + fqrn + "/api/v1/replicate/*/stderr", WebRequest::kGet),
      new UriHandlerJob());
    g_uri_map.Register(WebRequest(
      "/cvmfs/" + fqrn + "/api/v1/replicate/*/status", WebRequest::kGet),
      new UriHandlerJob());
    g_uri_map.Register(WebRequest("/cvmfs/" + fqrn + "/api/v1/replicate/new",
                                  WebRequest::kPost),
                       new UriHandlerReplicate(i->second));
  }

  struct mg_context *ctx;
  struct mg_callbacks callbacks;

  // List of options. Last element must be NULL.
  const char *options[] = {"num_threads", "2", "listening_ports", "8080", NULL};

  // Prepare callbacks structure. We have only one callback, the rest are NULL.
  memset(&callbacks, 0, sizeof(callbacks));
  callbacks.begin_request = begin_request_handler;

  // Start the web server.
  ctx = mg_start(&callbacks, NULL, options);
  // That's safe, our mongoose webserver doesn't spawn anything
  // There is a race here, TODO: patch in mongoose source code
  signal(SIGCHLD, SIG_DFL);

  // Wait until user hits "enter". Server is running in separate thread.
  // Navigating to http://localhost:8080 will invoke begin_request_handler().
  while (true)
    getchar();

  // Stop the server.
  mg_stop(ctx);

  return 0;
}
