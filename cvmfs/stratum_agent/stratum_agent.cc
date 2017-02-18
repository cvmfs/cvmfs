#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <map>
#include <string>
#include <vector>

#include "download.h"
#include "letter.h"
#include "logging.h"
#include "mongoose.h"
#include "options.h"
#include "signature.h"
#include "stratum_agent/uri_map.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/string.h"

using namespace std;

struct {
  string fqrn;
  signature::SignatureManager *signature_mgr;
  string cmd;
} user_data;


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
  download::DownloadManager *download_mgr;
  signature::SignatureManager *signature_mgr;
  OptionsManager *options_mgr;
  perf::Statistics *statistics;
};

map<string, RepositoryConfig *> g_repositories;
UriMap g_uri_map;

void ReadConfiguration() {
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


class UriHandlerReplicate : public UriHandler {
 public:
  explicit UriHandlerReplicate(const string &name) : name_(name) { }
  virtual void OnRequest(const struct mg_request_info *req_info,
                         struct mg_connection *conn)
  {
    WebReply::Send(WebReply::k200, "I handle this", conn);
    LogCvmfs(kLogCvmfs, kLogStdout, "I handle %s", name_.c_str());
  }

 private:
  string name_;
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

  /*char content[10000];

  char post_data[10000];
  post_data[0] = '\0';
  int post_data_len;
  post_data_len = mg_read(conn, post_data, sizeof(post_data));
  // TODO: trim final newline
  post_data[post_data_len-1] = '\0';
  letter::Failures retval_ltr;

  //printf("POST DATA %s\n", post_data);

  string message;
  string cert;
  letter::Letter letter(user_data.fqrn, post_data, user_data.signature_mgr);
  //printf("LETTER IS %p\nTEXT %s\n", &letter, letter.text().c_str());
  retval_ltr = letter.Verify(10, &message, &cert);
  string dec;
  Debase64(message, &dec);

  int stdin, stdout, stderr;
  Shell(&stdin, &stdout, &stderr);
  string cmd = user_data.cmd + ";exit \n";
  write(stdin, cmd.data(), cmd.length());
  string out;
  char c;
  mg_printf(conn,
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/plain\r\n\r\n");
  while (read(stdout, &c, 1) == 1) {
    if (c == '\n') {
      mg_printf(conn, "%s\n", out.c_str());
      out.clear();
    } else {
      out.push_back(c);
    }
  }

  // Prepare the message we're going to send
  //int content_length = snprintf(content, sizeof(content),
  //                              "Hello from mongoose! type: %s, letter %s "
  //                              "code %d, length %d\n",
  //                              request_info->request_method,
  //                              dec.c_str(),
  //                              retval_ltr,
  //                              post_data_len);



  // Send HTTP reply to the client
  //mg_printf(conn,
  //          "HTTP/1.1 200 OK\r\n"
  //          "Content-Type: text/plain\r\n"
  //          "Content-Length: %d\r\n"        // Always set Content-Length
  //          "\r\n"
  //          "%s",
  //          content_length, content);

  */
}



int main(int argc, char **argv) {
  ReadConfiguration();
  for (map<string, RepositoryConfig *>::const_iterator i =
       g_repositories.begin(), i_end = g_repositories.end(); i != i_end; ++i)
  {
    string fqrn = i->second->fqrn;
    g_uri_map.Register(WebRequest("/cvmfs/" + fqrn + "/api/v1/replicate/new",
                                  WebRequest::kPost),
                       new UriHandlerReplicate(fqrn));
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
  printf("CONTEXT IS NOW %p\n", ctx);

  // Wait until user hits "enter". Server is running in separate thread.
  // Navigating to http://localhost:8080 will invoke begin_request_handler().
  while (true)
    getchar();

  // Stop the server.
  mg_stop(ctx);

  return 0;
}
