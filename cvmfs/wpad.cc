/**
 * This file is part of the CernVM File System.
 */


#include "wpad.h"

#include <fcntl.h>

#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include "network/download.h"
#include "pacparser.h"
#include "statistics.h"
#include "util/logging.h"
#include "util/posix.h"
#include "util/string.h"

using namespace std;  // NOLINT

namespace download {

const char *kAutoPacLocation = "http://wpad/wpad.dat";

static int PrintPacError(const char *fmt, va_list argp) {
  char *msg = NULL;

  int retval = vasprintf(&msg, fmt, argp);
  assert(retval != -1);  // else: out of memory

  LogCvmfs(kLogDownload, kLogDebug | kLogSyslogErr, "(pacparser) %s", msg);
  free(msg);
  return retval;
}


static string PacProxy2Cvmfs(const string &pac_proxy,
                             const bool report_errors) {
  int log_flags = report_errors ? kLogDebug | kLogSyslogWarn : kLogDebug;
  if (pac_proxy == "")
    return "DIRECT";

  string cvmfs_proxy;
  vector<string> components = SplitString(pac_proxy, ';');
  for (unsigned i = 0; i < components.size(); ++i) {
    // Remove white spaces
    string next_proxy;
    for (unsigned j = 0; j < components[i].length(); ++j) {
      if ((components[i][j] != ' ') && (components[i][j] != '\t'))
        next_proxy.push_back(components[i][j]);
    }

    // No SOCKS support
    if (HasPrefix(next_proxy, "SOCKS", false)) {
      LogCvmfs(kLogDownload, log_flags,
               "no support for SOCKS proxy, skipping %s",
               next_proxy.substr(5).c_str());
      continue;
    }

    if ((next_proxy != "DIRECT") && !HasPrefix(next_proxy, "PROXY", false)) {
      LogCvmfs(kLogDownload, log_flags, "invalid proxy definition: %s",
               next_proxy.c_str());
      continue;
    }

    if (HasPrefix(next_proxy, "PROXY", false))
      next_proxy = next_proxy.substr(5);

    if (cvmfs_proxy == "")
      cvmfs_proxy = next_proxy;
    else
      cvmfs_proxy += ";" + next_proxy;
  }

  return cvmfs_proxy;
}


static bool ParsePac(const char *pac_data, const size_t size,
                     DownloadManager *download_manager, string *proxies) {
  *proxies = "";

  pacparser_set_error_printer(PrintPacError);
  bool retval = pacparser_init();
  if (!retval)
    return false;

  const string pac_string(pac_data, size);
  LogCvmfs(kLogDownload, kLogDebug, "PAC script is:\n%s", pac_string.c_str());
  retval = pacparser_parse_pac_string(pac_string.c_str());
  if (!retval) {
    pacparser_cleanup();
    return false;
  }

  // For every stratum 1: get proxy
  vector<string> host_list;
  vector<int> rtt;
  unsigned current_host;
  download_manager->GetHostInfo(&host_list, &rtt, &current_host);
  for (unsigned i = 0; i < host_list.size(); ++i) {
    size_t hostname_begin = 7;  // Strip http:// or file://
    size_t hostname_end = host_list[i].find_first_of(":/", hostname_begin);
    size_t hostname_len = (hostname_end == string::npos)
                              ? string::npos
                              : hostname_end - hostname_begin;
    const string hostname = (hostname_begin > host_list[i].length())
                                ? "localhost"
                                : host_list[i].substr(hostname_begin,
                                                      hostname_len);
    const string url = host_list[i] + "/.cvmfspublished";

    // pac_proxy is freed by JavaScript GC
    char *pac_proxy = pacparser_find_proxy(url.c_str(), hostname.c_str());
    if (pac_proxy == NULL) {
      pacparser_cleanup();
      return false;
    }
    if (*proxies == "") {
      *proxies = PacProxy2Cvmfs(pac_proxy, true);
      if (*proxies == "") {
        LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
                 "no valid proxy found (%s returned from pac file)", pac_proxy);
        pacparser_cleanup();
        return false;
      }
    } else {
      const string alt_proxies = PacProxy2Cvmfs(pac_proxy, false);
      if (*proxies != alt_proxies) {
        LogCvmfs(kLogDownload, kLogDebug,
                 "proxy settings for host %s differ from proxy settings for "
                 "other hosts (%s / %s). Not using proxy setting %s.",
                 host_list[i].c_str(), proxies->c_str(), alt_proxies.c_str(),
                 alt_proxies.c_str());
      }
    }
  }

  pacparser_cleanup();
  return true;
}


string AutoProxy(DownloadManager *download_manager) {
  char *http_env = getenv("http_proxy");
  if (http_env) {
    LogCvmfs(kLogDownload, kLogDebug | kLogSyslog,
             "CernVM-FS: "
             "using HTTP proxy server(s) %s from http_proxy environment",
             http_env);
    return string(http_env);
  }

  vector<string> pac_paths;
  char *pac_env = getenv("CVMFS_PAC_URLS");
  if (pac_env != NULL)
    pac_paths = SplitString(pac_env, ';');

  // Try downloading from each of the PAC URLs
  for (unsigned i = 0; i < pac_paths.size(); ++i) {
    if (pac_paths[i] == "auto") {
      LogCvmfs(kLogDownload, kLogDebug, "resolving auto proxy config to %s",
               kAutoPacLocation);
      pac_paths[i] = string(kAutoPacLocation);
    }
    LogCvmfs(kLogDownload, kLogDebug, "looking for proxy config at %s",
             pac_paths[i].c_str());
    cvmfs::MemSink pac_memsink;
    download::JobInfo download_pac(&pac_paths[i], false, false, NULL,
                                   &pac_memsink);
    int retval = download_manager->Fetch(&download_pac);
    if (retval == download::kFailOk) {
      string proxies;
      retval = ParsePac(reinterpret_cast<char *>(pac_memsink.data()),
                        pac_memsink.pos(),
                        download_manager,
                        &proxies);
      if (!retval) {
        LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
                 "failed to parse pac file %s", pac_paths[i].c_str());
      } else {
        if (proxies != "") {
          LogCvmfs(kLogDownload, kLogDebug | kLogSyslog,
                   "CernVM-FS: "
                   "using HTTP proxy server(s) %s from pac file %s",
                   proxies.c_str(), pac_paths[i].c_str());
          return proxies;
        }
      }

      LogCvmfs(kLogDownload, kLogDebug, "no proxy settings found in %s",
               pac_paths[i].c_str());
    }
  }

  return "";
}


string ResolveProxyDescription(const string &cvmfs_proxies,
                               const std::string &path_fallback_cache,
                               DownloadManager *download_manager) {
  if ((cvmfs_proxies == "") || (cvmfs_proxies.find("auto") == string::npos))
    return cvmfs_proxies;

  int empty_auto = -1;
  vector<string> lb_groups = SplitString(cvmfs_proxies, ';');
  for (unsigned i = 0; i < lb_groups.size(); ++i) {
    if (lb_groups[i] != "auto")
      continue;

    lb_groups[i] = AutoProxy(download_manager);
    if (lb_groups[i].empty())
      empty_auto = static_cast<int>(i);
  }

  if (empty_auto != -1)
    lb_groups.erase(lb_groups.begin() + static_cast<unsigned>(empty_auto));
  string discovered_proxies = JoinStrings(lb_groups, ";");

  if (!path_fallback_cache.empty()) {
    if (empty_auto != -1) {
      string cached_proxies;
      int fd = open(path_fallback_cache.c_str(), O_RDONLY);
      if (fd >= 0) {
        bool retval = SafeReadToString(fd, &cached_proxies);
        close(fd);
        if (retval) {
          LogCvmfs(kLogDownload, kLogSyslog | kLogDebug,
                   "using cached proxy settings from %s",
                   path_fallback_cache.c_str());
          return cached_proxies;
        }
      }
    } else {
      bool retval = SafeWriteToFile(discovered_proxies, path_fallback_cache,
                                    0660);
      if (!retval) {
        LogCvmfs(kLogDownload, kLogSyslogWarn | kLogDebug,
                 "failed to write proxy settings into %s",
                 path_fallback_cache.c_str());
      }
    }
  }

  return discovered_proxies;
}


static void AltCvmfsLogger(const LogSource source, const int mask,
                           const char *msg) {
  FILE *log_output = NULL;
  if (mask & kLogStdout)
    log_output = stdout;
  else if (mask & kLogStderr || mask & kLogSyslogWarn || mask & kLogSyslogErr)
    log_output = stderr;
  if (log_output)
    fprintf(log_output, "%s\n", msg);
}


int MainResolveProxyDescription(int argc, char **argv) {
  SetAltLogFunc(AltCvmfsLogger);
  if (argc < 4) {
    LogCvmfs(kLogDownload, kLogStderr, "arguments missing");
    return 1;
  }
  perf::Statistics statistics;
  string proxy_configuration = argv[2];
  string host_list = argv[3];

  DownloadManager download_manager(
      1, perf::StatisticsTemplate("pac", &statistics));
  download_manager.SetHostChain(host_list);
  string resolved_proxies = ResolveProxyDescription(proxy_configuration, "",
                                                    &download_manager);

  LogCvmfs(kLogDownload, kLogStdout, "%s", resolved_proxies.c_str());
  return resolved_proxies == "";
}

}  // namespace download
