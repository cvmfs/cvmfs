/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"

#include <stdio.h>

#include <string>

#include "download.h"
#include "swissknife_pull.h"
#include "swissknife.h"
#include "logging.h"
#include "signature.h"
#include "statistics.h"
#include "util.h"


using namespace std;  // NOLINT

namespace swissknife {
download::DownloadManager *g_download_manager;
signature::SignatureManager *g_signature_manager;
perf::Statistics *g_statistics;
void Usage() {
    LogCvmfs(kLogCvmfs, kLogStderr, "Usage:\n"
    "cvmfs_preload <Stratum 0 URL>\n"
    "              <cache directory>\n"
    "              <public key>\n"
    "              <fully qualified repository name>\n"
    "              <directory for temporary files>\n"
    "              <path to dirtab file>\n\n");
}
}  // namespace swissknife


int main(int argc, char *argv[]) {
  if (argc < 7) {
    printf("Not enough arguments: %d\n\n", argc);
    swissknife::Usage();
    return 1;
  }
  string url = argv[1];
  string alien_cache_dir = argv[2];
  string public_key_path = argv[3];
  string fqrn = argv[4];
  string tempdir = argv[5];
  string dirtab_file = argv[6];
  string num_threads = "4";

  // first create the alien cache
  int retval = MkdirDeep(alien_cache_dir, 0770);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to create %s", alien_cache_dir.c_str());
    return 1;
  }
  retval = MakeCacheDirectories(alien_cache_dir, 0770);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to create cache skeleton");
    return 1;
  }

  // now launch swissknife_pull
  swissknife::g_download_manager = new download::DownloadManager();
  swissknife::g_signature_manager = new signature::SignatureManager();
  swissknife::g_statistics = new perf::Statistics();

  // load the command
  swissknife::ArgumentList args;
  args['c'] = NULL;
  args['u'] = &url;
  args['r'] = &alien_cache_dir;
  args['k'] = &public_key_path;
  args['m'] = &fqrn;
  args['x'] = &tempdir;
  args['d'] = &dirtab_file;
  args['n'] = &num_threads;

  return swissknife::CommandPull().Main(args);
}
