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
  string fqrn;
  string stratum0_url;
  download::DownloadManager *download_mgr;
  signature::SignatureManager *signature_mgr;
  OptionsManager *options_mgr;
  perf::Statistics *statistics;
};

map<string, RepositoryConfig *> g_repositories;
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

    string uuid = cvmfs::Uuid::CreateOneTime();
    WebReply::Send(WebReply::k200, "{\"job_id\":\"" + uuid + "\"}", conn);
  }

 private:
  static const unsigned kMaxPostData = 32 * 1024;  // 32kB
  static const unsigned kTimeoutLetter = 180;  // 3 minutes

  string MkJsonError(const string &msg) {
    return "{\"error\": \"" + msg + "\"}";
  }

  RepositoryConfig *config_;
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

  // Wait until user hits "enter". Server is running in separate thread.
  // Navigating to http://localhost:8080 will invoke begin_request_handler().
  while (true)
    getchar();

  // Stop the server.
  mg_stop(ctx);

  return 0;
}
