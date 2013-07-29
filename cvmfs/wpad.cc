/**
 * This file is part of the CernVM File System.
 */

#include "config.h"
#include "wpad.h"

#include <cstdlib>
#include <vector>

#include "download.h"
#include "logging.h"
#include "util.h"

using namespace std;  // NOLINT

namespace download {

static string ParsePac(const char *pac_string, const size_t size,
                       DownloadManager *download_manager)
{
  return "DIRECT";
}


string AutoProxy(DownloadManager *download_manager) {
  char *http_env = getenv("http_proxy");
  if (http_env) {
    LogCvmfs(kLogDownload, kLogDebug | kLogSyslog, "CernVM-FS: "
             "using HTTP proxy server(s) %s from http_proxy environment",
             http_env);
    return string(http_env);
  }

  vector<string> pac_paths;
  char *pac_env = getenv("PAC_URLS");
  if (pac_env != NULL)
    pac_paths = SplitString(pac_env, ';');
  pac_paths.push_back("http://wpad/wpad.dat");
  pac_paths.push_back("http://wlcg-wpad.cern.ch/wpad.dat");

  // Try downloading from each of the PAC URLs
  for (unsigned i = 0; i < pac_paths.size(); ++i) {
    LogCvmfs(kLogDownload, kLogDebug, "looking for proxy config at %s",
             pac_paths[i].c_str());
    download::JobInfo download_pac(&pac_paths[i], false, false, NULL);
    int retval = download_manager->Fetch(&download_pac);
    if (retval == download::kFailOk) {
      const string proxies =
        ParsePac(download_pac.destination_mem.data,
                 download_pac.destination_mem.size,
                 download_manager);
      free(download_pac.destination_mem.data);
      if (proxies != "") {
        LogCvmfs(kLogDownload, kLogDebug | kLogSyslog, "CernVM-FS: "
                 "using HTTP proxy server(s) %s from pac file %s",
                 proxies.c_str(), pac_paths[i].c_str());
        return proxies;
      }
    }
  }

  return "";
}

}  // namespace download
