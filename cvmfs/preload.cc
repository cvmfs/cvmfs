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
#include "util/posix.h"
#include "util/string.h"
#include "uuid.h"


using namespace std;  // NOLINT

const char *kVersion = VERSION;
const int kDefaultPreloaderTimeout = 10;
const int kDefaultPreloaderRetries = 2;

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
    "              [-t <timeout in seconds (default %d)>]\n"
    "              [-a <number of retries (default %d)>]\n"
    "              [-x <directory for temporary files>]\n\n",
    kVersion, kDefaultPreloaderTimeout, kDefaultPreloaderRetries);
}
}  // namespace swissknife

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

const char gCernIt4PublicKey[] =
  "-----BEGIN PUBLIC KEY-----\n"
  "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAzlAraXimfJP5ie0KtDAE\n"
  "rNUU5d9bzst+kqfhnb0U0OUtmCIbsueaDlbMmTdRSHMr+T0jI8i9CZxJwtxDqll+\n"
  "UuB3Li2hYBhk0tYTy29JJYvofVULvrw1kMSLKyTWnV30/MHjYxhLHoZWfdepTjVg\n"
  "lM0rP58K10wR3Z/AaaikOcy4z6P/MHs9ES1jdZqEBQEmmzKw5nf7pfU2QuVWJrKP\n"
  "wZ9XeYDzipVbMc1zaLEK0slE+bm2ge/Myvuj/rpYKT+6qzbasQg62abGFuOrjgKI\n"
  "X4/BVnilkhUfH6ssRKw4yehlKG1M5KJje2+y+iVvLbfoaw3g1Sjrf4p3Gq+ul7AC\n"
  "PwIDAQAB\n"
  "-----END PUBLIC KEY-----\n";

const char gCernIt5PublicKey[] =
  "-----BEGIN PUBLIC KEY-----\n"
  "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAqFzLLZAg2xmHJLbbq0+N\n"
  "eYtjRDghUK5mYhARndnC3skFVowDTiqJVc9dIDX5zuxQ9HyC0iKM1HbvN64IH/Uf\n"
  "qoXLyZLiXbFwpg6BtEJxwhijdZCiCC5PC//Bb7zSFIVZvWjndujr6ejaY6kx3+jI\n"
  "sU1HSJ66pqorj+D1fbZCziLcWbS1GzceZ7aTYYPUdGZF1CgjBK5uKrEAoBsPgjWo\n"
  "+YOEkjskY7swfhUzkCe0YyMyAaS0gsWgYrY2ebrpauFFqKxveKWjDVBTGcwDhiBX\n"
  "60inUgD6CJXhUpvGHfU8V7Bv6l7dmyzhq/Bk2kRC92TIvxfaHRmS7nuknUY0hW6t\n"
  "2QIDAQAB\n"
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
  args['n'].Reset(new string("4"));

  string option_string = "u:r:k:m:x:d:n:t:a:vh";
  int c;
  while ((c = getopt(argc, argv, option_string.c_str())) != -1) {
    if ((c == 'v') || (c == 'h')) {
      swissknife::Usage();
      return 0;
    }
    args[c].Reset(new string(optarg));
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
    args['m'].Reset(new string(fqrn));
  }

  if (args.find('x') == args.end())
    args['x'].Reset(new string(*args['r'] + "/txn"));

  const string cache_directory = *args['r'];
  const string fqrn = *args['m'];
  const string dirtab =
    (args.find('d') == args.end()) ?  "/dev/null" : *args['d'];
  const string dirtab_in_cache = cache_directory + "/dirtab." + fqrn;

  // Default network parameters: 5 seconds timeout, 2 retries
  if (args.find('t') == args.end())
    args['t'].Reset(new string(StringifyInt(kDefaultPreloaderTimeout)));
  if (args.find('a') == args.end())
    args['a'].Reset(new string(StringifyInt(kDefaultPreloaderRetries)));

  // first create the alien cache
  const string& alien_cache_dir = *(args['r']);
  retval = MkdirDeep(alien_cache_dir, 0770);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to create %s",
      alien_cache_dir.c_str());
    return 1;
  }
  retval = MakeCacheDirectories(alien_cache_dir, 0770);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to create cache skeleton");
    return 1;
  }

  // if there is no specified public key file we dump the cern.ch public key in
  // the temporary directory
  string cern_pk_base_path = *args['x'];
  string cern_pk_it1_path  = cern_pk_base_path + "/cern-it1.cern.ch.pub";
  string cern_pk_it4_path  = cern_pk_base_path + "/cern-it4.cern.ch.pub";
  string cern_pk_it5_path  = cern_pk_base_path + "/cern-it5.cern.ch.pub";
  bool keys_created = false;
  if (args.find('k') == args.end()) {
    keys_created = true;
    assert(CopyMem2Path(
      reinterpret_cast<const unsigned char*>(gCernIt1PublicKey),
      sizeof(gCernIt1PublicKey), cern_pk_it1_path));
    assert(CopyMem2Path(
      reinterpret_cast<const unsigned char*>(gCernIt4PublicKey),
      sizeof(gCernIt4PublicKey), cern_pk_it4_path));
    assert(CopyMem2Path(
      reinterpret_cast<const unsigned char*>(gCernIt5PublicKey),
      sizeof(gCernIt5PublicKey), cern_pk_it5_path));
    char path_separator = ':';
    args['k'].Reset(new string(cern_pk_it1_path + path_separator +
                           cern_pk_it4_path + path_separator +
                           cern_pk_it5_path));
  }

  // now launch swissknife_pull
  swissknife::g_download_manager = new download::DownloadManager();
  swissknife::g_signature_manager = new signature::SignatureManager();
  swissknife::g_statistics = new perf::Statistics();

  // load the command
  if (HasDirtabChanged(dirtab, dirtab_in_cache)) {
    LogCvmfs(kLogCvmfs, kLogStdout, "CernVM-FS: new dirtab, forced run");
    args['z'].Reset();  // look into existing catalogs, too
  }
  args['c'].Reset();
  retval = swissknife::CommandPull().Main(args);

  // Copy dirtab file
  if (retval == 0) {
    CopyPath2Path(dirtab, dirtab_in_cache);
  }

  // Create cache uuid if not present
  cvmfs::Uuid *uuid = cvmfs::Uuid::Create(cache_directory + "/uuid");
  if (uuid == NULL) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Warning: failed to create %s/uuid",
             cache_directory.c_str());
  }
  delete uuid;

  if (keys_created) {
    unlink(cern_pk_it1_path.c_str());
    unlink(cern_pk_it4_path.c_str());
    unlink(cern_pk_it5_path.c_str());
  }

  return retval;
}
