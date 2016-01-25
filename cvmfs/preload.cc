/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"

#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include <string>

#include "compression.h"
#include "download.h"
#include "logging.h"
#include "signature.h"
#include "statistics.h"
#include "swissknife.h"
#include "swissknife_pull.h"
#include "util.h"


using namespace std;  // NOLINT

const char *kVersion = VERSION;

namespace swissknife {
download::DownloadManager *g_download_manager;
signature::SignatureManager *g_signature_manager;
perf::Statistics *g_statistics;
void Usage() {
    LogCvmfs(kLogCvmfs, kLogStderr, "Version: %s\n\n"
    "Usage:\n"
    "cvmfs_preload -u <Stratum 0 URL>\n"
    "              -r <alien cache directory>\n"
    "              [-d <path to dirtab file>]\n"
    "              [-k <public key>]\n"
    "              [-m <fully qualified repository name>]\n"
    "              [-n <num of parallel download threads>]\n"
    "              [-x <directory for temporary files>]\n\n", kVersion);
}
}  // namespace swissknife

const char gCernPublicKey[] =
  "-----BEGIN PUBLIC KEY-----\n"
  "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAukBusmYyFW8KJxVMmeCj\n"
  "N7vcU1mERMpDhPTa5PgFROSViiwbUsbtpP9CvfxB/KU1gggdbtWOTZVTQqA3b+p8\n"
  "g5Vve3/rdnN5ZEquxeEfIG6iEZta9Zei5mZMeuK+DPdyjtvN1wP0982ppbZzKRBu\n"
  "BbzR4YdrwwWXXNZH65zZuUISDJB4my4XRoVclrN5aGVz4PjmIZFlOJ+ytKsMlegW\n"
  "SNDwZO9z/YtBFil/Ca8FJhRPFMKdvxK+ezgq+OQWAerVNX7fArMC+4Ya5pF3ASr6\n"
  "3mlvIsBpejCUBygV4N2pxIcPJu/ZDaikmVvdPTNOTZlIFMf4zIP/YHegQSJmOyVp\n"
  "HQIDAQAB\n"
  "-----END PUBLIC KEY-----\n";

const char gCernIt1PublicKey[] =
  "-----BEGIN PUBLIC KEY-----\n"
  "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAo8uKvscgW7FNxzb65Uhm\n"
  "yr8jPJiyrl2kVzb/hhgdfN14C0tCbfFoE6ciuZFg+9ytLeiL9pzM96gSC+atIFl4\n"
  "7wTgtAFO1W4PtDQBwA/IG2bnwvNrzk19ob0JYhjZlS9tYKeh7TKCub55+vMwcEbP\n"
  "urzo3WSNCzJngiGMh1vM5iSlGLpCdSGzdwxLGwc1VjRM7q3KAd7M7TJCynKqXZPX\n"
  "R2xiD6I/p4xv39AnwphCFSmDh0MWE1WeeNHIiiveikvvN+l8d/ZNASIDhKNCsz6o\n"
  "aFDsGXvjGy7dg43YzjSSYSFGUnONtl5Fe6y4bQZj1LEPbeInW334MAbMwYF4LKma\n"
  "yQIDAQAB\n"
  "-----END PUBLIC KEY-----\n";

const char gCernIt2PublicKey[] =
  "-----BEGIN PUBLIC KEY-----\n"
  "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAkX6+mj6/X5yLV9uHt56l\n"
  "ZK1uLMueEULUhSCRrLj+9qz3EBMsANCjzfdabllKqWX/6qIfqppKVBwScF38aRnC\n"
  "vhlVGYtDgiqM1tfa1tA6BF7HUZQ1R1lU01tP0iYGhNTlTfY+fMAZDeerGDckT8cl\n"
  "NAQuICFUKy6w6aar8Sf3mpUC/hXVD2QUESmFgQ0SZhhW3eIB1xEoRxW0ieO6AidF\n"
  "qxmAxrB4H4+7i9O+B7Wf0VJQLSzP5bvIzx7xoqs3aUlnuzGFOaI8zypMvSvycSUb\n"
  "xhLsYbTgnlqSI/SPehMeQeEirhhKPaA+TsVSvxuqCJNoQ/wPBEG0HR+XkTsO9/sH\n"
  "FQIDAQAB\n"
  "-----END PUBLIC KEY-----\n";

const char gCernIt3PublicKey[] =
  "-----BEGIN PUBLIC KEY-----\n"
  "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAosLXrVkA4p6IjQj6rUNM\n"
  "odr9oWB1nL3tWPKyPhS7mqAg+J4EW9m4ka/98PXi6jS/b1i/QLP9oGXlxJpugT1E\n"
  "jaKh/I0tY7Cvf19mX3uoyS4omWRqZqopQdeLOvuqiCip23YyO3lK4Yzkq1E58JGi\n"
  "WLGe5UJ8kY8Bko79dGNsHsU01pAaI0QN/fSmwhHQqfpMv62cAkqB9GSilRalxf3+\n"
  "mDtJhYBdaDKbB5+QDqh40HcH838H+GcQLXxdT5ogdchjeldZJzsTwEhRq3yDcYr3\n"
  "ie6ocWVLchQx9CKpxPufRTEpuo3BPMqdTxhZJZWPG27I/FSWnUmd0auirFY51Rw6\n"
  "9wIDAQAB\n"
  "-----END PUBLIC KEY-----\n";


static char CheckParameters(const string &params,
                                   swissknife::ArgumentList *args) {
  for (unsigned i = 0; i < params.length(); ++i) {
    char param = params[i];
    if (args->find(param) == args->end()) {
      return param;
    }
  }
  return '\0';
}

static bool HasDirtabChanged(const string &dirtab_src, const string &dirtab_dst)
{
  bool retval;
  shash::Any hash_src(shash::kMd5);
  shash::Any hash_dst(shash::kMd5);
  retval = shash::HashFile(dirtab_src, &hash_src);
  if (!retval)
    return true;
  retval = shash::HashFile(dirtab_dst, &hash_dst);
  if (!retval)
    return true;
  return hash_src != hash_dst;
}


int main(int argc, char *argv[]) {
  int retval;

  // load some default arguments
  swissknife::ArgumentList args;
  string default_num_threads = "4";
  args['n'] = &default_num_threads;

  string option_string = "u:r:k:m:x:d:n:vh";
  int c;
  while ((c = getopt(argc, argv, option_string.c_str())) != -1) {
    if ((c == 'v') || (c == 'h')) {
      swissknife::Usage();
      return 0;
    }
    args[c] = new string(optarg);
  }

  // check all mandatory parameters are included
  string necessary_params = "ur";
  char result;
  if ((result = CheckParameters(necessary_params, &args)) != '\0') {
    printf("Argument not included but necessary: -%c\n\n", result);
    swissknife::Usage();
    return 2;
  }

  if (args.find('m') == args.end()) {
    string fqrn = GetFileName(*args['u']);
    LogCvmfs(kLogCvmfs, kLogStdout, "CernVM-FS: guessing fqrn from URL: %s",
             fqrn.c_str());
    args['m'] = new string(fqrn);
  }

  if (args.find('x') == args.end())
    args['x'] = new string(*args['r'] + "/txn");

  const string cache_directory = *args['r'];
  const string fqrn = *args['m'];
  const string dirtab =
    (args.find('d') == args.end()) ?  "/dev/null" : *args['d'];
  const string dirtab_in_cache = cache_directory + "/dirtab." + fqrn;

  // Default network parameters: 5 seconds timeout, 2 retries
  args['t'] = new string("5");
  args['a'] = new string("2");

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
  string cern_pk_base_path = *args['x'];
  string cern_pk_path      = cern_pk_base_path + "/cern.ch.pub";
  string cern_pk_it1_path  = cern_pk_base_path + "/cern-it1.cern.ch.pub";
  string cern_pk_it2_path  = cern_pk_base_path + "/cern-it2.cern.ch.pub";
  string cern_pk_it3_path  = cern_pk_base_path + "/cern-it3.cern.ch.pub";
  bool keys_created = false;
  if (args.find('k') == args.end()) {
    keys_created = true;
    assert(CopyMem2Path(reinterpret_cast<const unsigned char*>(gCernPublicKey),
      sizeof(gCernPublicKey), cern_pk_path));
    assert(CopyMem2Path(
      reinterpret_cast<const unsigned char*>(gCernIt1PublicKey),
      sizeof(gCernIt1PublicKey), cern_pk_it1_path));
    assert(CopyMem2Path(
      reinterpret_cast<const unsigned char*>(gCernIt2PublicKey),
      sizeof(gCernIt2PublicKey), cern_pk_it2_path));
    assert(CopyMem2Path(
      reinterpret_cast<const unsigned char*>(gCernIt3PublicKey),
      sizeof(gCernIt3PublicKey), cern_pk_it3_path));
    char path_separator = ':';
    args['k'] = new string(cern_pk_path     + path_separator +
                           cern_pk_it1_path + path_separator +
                           cern_pk_it2_path + path_separator +
                           cern_pk_it3_path);
  }

  // now launch swissknife_pull
  swissknife::g_download_manager = new download::DownloadManager();
  swissknife::g_signature_manager = new signature::SignatureManager();
  swissknife::g_statistics = new perf::Statistics();

  // load the command
  if (HasDirtabChanged(dirtab, dirtab_in_cache)) {
    LogCvmfs(kLogCvmfs, kLogStdout, "CernVM-FS: new dirtab, forced run");
    args['z'] = NULL;  // look into existing catalogs, too
  }
  args['c'] = NULL;
  retval = swissknife::CommandPull().Main(args);

  // Copy dirtab file
  if (retval == 0) {
    CopyPath2Path(dirtab, dirtab_in_cache);
  }

  if (keys_created) {
    unlink(cern_pk_path.c_str());
    unlink(cern_pk_it1_path.c_str());
    unlink(cern_pk_it2_path.c_str());
    unlink(cern_pk_it3_path.c_str());
  }

  return retval;
}
