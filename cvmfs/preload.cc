/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"

#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include <string>

#include "download.h"
#include "logging.h"
#include "signature.h"
#include "statistics.h"
#include "swissknife.h"
#include "swissknife_pull.h"
#include "util.h"


using namespace std;  // NOLINT

namespace swissknife {
download::DownloadManager *g_download_manager;
signature::SignatureManager *g_signature_manager;
perf::Statistics *g_statistics;
void Usage() {
    LogCvmfs(kLogCvmfs, kLogStderr, "Usage:\n"
    "cvmfs_preload -u <Stratum 0 URL>\n"
    "              -r <alien cache directory>\n"
    "              -k <public key>\n"
    "              -m <fully qualified repository name>\n"
    "              -x <directory for temporary files>\n"
    "              -d <path to dirtab file>\n\n");
}
}  // namespace swissknife

const string CERN_PUBLIC_KEY =
  "-----BEGIN PUBLIC KEY-----\n"
  "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAukBusmYyFW8KJxVMmeCj\n"
  "N7vcU1mERMpDhPTa5PgFROSViiwbUsbtpP9CvfxB/KU1gggdbtWOTZVTQqA3b+p8\n"
  "g5Vve3/rdnN5ZEquxeEfIG6iEZta9Zei5mZMeuK+DPdyjtvN1wP0982ppbZzKRBu\n"
  "BbzR4YdrwwWXXNZH65zZuUISDJB4my4XRoVclrN5aGVz4PjmIZFlOJ+ytKsMlegW\n"
  "SNDwZO9z/YtBFil/Ca8FJhRPFMKdvxK+ezgq+OQWAerVNX7fArMC+4Ya5pF3ASr6\n"
  "3mlvIsBpejCUBygV4N2pxIcPJu/ZDaikmVvdPTNOTZlIFMf4zIP/YHegQSJmOyVp\n"
  "HQIDAQAB\n"
  "-----END PUBLIC KEY-----\n";

char check_parameters(const string &params, swissknife::ArgumentList *args) {
  for (unsigned i = 0; i < params.length(); ++i) {
    char param = params[i];
    if (args->find(param) == args->end()) {
      return param;
    }
  }
  return '\0';
}

int main(int argc, char *argv[]) {
  if (argc < 7) {
    printf("Not enough arguments: %d\n\n", argc);
    swissknife::Usage();
    return 1;
  }
  int retval;

  // load some default arguments
  swissknife::ArgumentList args;
  string default_num_threads = "4";
  args['n'] = &default_num_threads;

  string option_string = "u:r:k:m:x:d:n:";
  int c;
  while ((c = getopt(argc, argv, option_string.c_str())) != -1) {
    args[c] = new string(optarg);
  }

  // check all mandatory parameters are included
  string necessary_params = "urm";
  char result;
  if ((result = check_parameters(necessary_params, &args)) != '\0') {
    printf("Argument not included but necessary: -%c\n\n", result);
    swissknife::Usage();
    return 2;
  }

  if (args.find('x') == args.end())
    args['x'] = new string(*args['r'] + "/txn");

  // first create the alien cache
  string *alien_cache_dir = args['r'];
  retval = MkdirDeep(*alien_cache_dir, 0770);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to create %s",
      alien_cache_dir->c_str());
    return 1;
  }
  retval = MakeCacheDirectories(*alien_cache_dir, 0770);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to create cache skeleton");
    return 1;
  }

  // if there is no specified public key file we dump the cern.ch public key in
  // the temporary directory
  string cern_pk_path = *args['x'] + "/cern.ch.pub";
  if (args.find('k') == args.end()) {
    FILE *cern_pk_file = fopen(cern_pk_path.c_str(), "w");
    assert(cern_pk_file);
    retval = fwrite(CERN_PUBLIC_KEY.c_str(), sizeof(char),
                    CERN_PUBLIC_KEY.length(), cern_pk_file);
    assert(retval);
    retval = fclose(cern_pk_file);
    assert(retval == 0);
    retval = chmod(cern_pk_path.c_str(), 0644);
    assert(retval == 0);
    args['k'] = &cern_pk_path;
  }

  // now launch swissknife_pull
  swissknife::g_download_manager = new download::DownloadManager();
  swissknife::g_signature_manager = new signature::SignatureManager();
  swissknife::g_statistics = new perf::Statistics();

  // load the command
  args['c'] = NULL;
  return swissknife::CommandPull().Main(args);
}
