/**
 * This file is part of the CernVM File System.
 */

#include <errno.h>
#include <getopt.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <fstream>
#include <vector>

#include "cvmfs_config.h"
#include "export_plugin/fs_traversal.h"
#include "export_plugin/fs_traversal_interface.h"
#include "logging.h"
#include "util/string.h"

// Taken from fsck
enum Errors {
  kErrorOk = 0,
  kErrorFixed = 1,
  kErrorReboot = 2,
  kErrorUnfixed = 4,
  kErrorOperational = 8,
  kErrorUsage = 16,
};


static void Usage() {
  LogCvmfs(kLogCvmfs, kLogStdout,
       "CernVM File System Shrinkwrapper, version %s\n\n"
       "This tool takes a cvmfs repository and outputs\n"
       "to a destination files system.\n"
       "Usage: cvmfs_shrinkwrap "
       "[-r][-sbcf][-dxyz][-t|b][-jrg]\n"
        "Options:\n"
        " -r --repo        Repository name [required]\n"
        " -s --src-type    Source filesystem type [default:cvmfs]\n"
        " -b --src-base    Source base location [default:/cvmfs/]\n"
        " -c --src-cache   Source cache\n"
        " -f --src-config  Source config [default:$REPO.config]\n"
        " -d --dest-type   Dest filesystem type [default:posix]\n"
        " -x --dest-base   Dest base [default:/tmp/cvmfs/]\n"
        " -y --dest-cache  Dest cache [default:$BASE/.data]\n"
        " -z --dest-config Dest config\n"
        " -t --spec-file   Specification file\n"
        " -b --base        Base directory in repo [default=/]\n"
        " -j --threads     Number of concurrent copy threads [default:0]\n"
        " -r --retries     Number of retries on copying file [default:0]\n"
        " -g --gc          Perform garbage collection on destination\n",
           VERSION);
}

int main(int argc, char **argv) {
  // The starting location for the traversal in src
  // Default value is the base directory (only used if not trace provided)
  char *repo = NULL;

  char *src_base = NULL;
  char *src_cache = NULL;
  char *src_type = NULL;
  char *src_config = NULL;

  char *dest_base = NULL;
  char *dest_cache = NULL;
  char *dest_type = NULL;
  char *dest_config = NULL;

  char *base = NULL;
  char *spec_file = NULL;

  bool garbage_collection = false;

  unsigned num_parallel = 0;
  unsigned retries = 0;

  int c;

  static struct option long_opts[] = {
      /* All of the options require an argument */
      {"help",        no_argument, 0, 'h'},
      {"base",        required_argument, 0, 'p'},
      {"repo",        required_argument, 0, 'r'},
      {"src-type",    required_argument, 0, 's'},
      {"src-base",    required_argument, 0, 'b'},
      {"src-cache",   required_argument, 0, 'c'},
      {"src-config",  required_argument, 0, 'f'},
      {"dest-type",   required_argument, 0, 'd'},
      {"dest-base",   required_argument, 0, 'x'},
      {"dest-cache",  required_argument, 0, 'y'},
      {"dest-config", required_argument, 0, 'z'},
      {"spec-file",   required_argument, 0, 't'},
      {"threads",     required_argument, 0, 'j'},
      {"retries",     required_argument, 0, 'n'},
      {"gc",          required_argument, 0, 'g'},
      {0, 0, 0, 0}
    };

  static const char short_opts[] = "hp:b:s:r:c:f:d:x:y:t:j:n:g";

  while ((c = getopt_long(argc, argv, short_opts, long_opts, NULL)) >= 0) {
    switch (c) {
      case 'h':
        Usage();
        return kErrorOk;
      case 'p':
        base = strdup(optarg);
        if (spec_file) {
          LogCvmfs(kLogCvmfs, kLogStdout,
                   "Only allowed to specify either base dir or trace file");
          return kErrorUsage;
        }
        break;
      case 'r':
        repo = strdup(optarg);
        break;
      case 's':
        src_type = strdup(optarg);
        break;
      case 'b':
        src_base = strdup(optarg);
        break;
      case 'c':
        src_cache = strdup(optarg);
        break;
      case 'f':
        src_config = strdup(optarg);
        break;
      case 'd':
        dest_type = strdup(optarg);
        break;
      case 'x':
        dest_base = strdup(optarg);
        break;
      case 'y':
        dest_cache = strdup(optarg);
        break;
      case 'z':
        dest_config = strdup(optarg);
        break;
      case 't':
        spec_file = strdup(optarg);
        if (base) {
          LogCvmfs(kLogCvmfs, kLogStdout,
                   "Only allowed to specify either base dir or trace file");
          return kErrorUsage;
        }
        break;
      case 'j':
        num_parallel = atoi(optarg);
        if (num_parallel < 1) {
          LogCvmfs(kLogCvmfs, kLogStdout,
                   "There is at least one worker thread required");
          return kErrorUsage;
        }
        break;
      case 'n':
        retries = atoi(optarg);
        break;
      case 'g':
        garbage_collection = true;
        break;
      case '?':
      default:
        Usage();
        return kErrorUsage;
    }
  }

  if (!src_type) {
    src_type = strdup("cvmfs");
  }

  if (!dest_type) {
    dest_type = strdup("posix");
  }

  struct fs_traversal *src = shrinkwrap::FindInterface(src_type);
  if (!src) {
    return 1;
  }
  src->context_ = src->initialize(repo, src_base, src_cache,
    num_parallel, src_config);
  if (!src->context_) {
    LogCvmfs(kLogCvmfs, kLogStdout,
      "Unable to initialize src: type %s", src_type);
    return 1;
  }

  struct fs_traversal *dest = shrinkwrap::FindInterface(dest_type);
  if (!dest) {
    return 1;
  }
  dest->context_ = dest->initialize(repo, dest_base, dest_cache,
    num_parallel, dest_config);
  if (!dest->context_) {
    LogCvmfs(kLogCvmfs, kLogStdout,
      "Unable to initialize src: type %s", dest_type);
    return 1;
  }

  dest->archive_provenance(src->context_, dest->context_);

  if (!base) {
    base = strdup("");
  }

  int result = shrinkwrap::SyncInit(src, dest, base, spec_file,
                                    num_parallel, retries);

  src->finalize(src->context_);
  if (garbage_collection) {
    shrinkwrap::GarbageCollect(dest);
  }
  dest->finalize(dest->context_);

  delete src;
  delete dest;
  free(repo);

  free(src_base);
  free(src_cache);
  free(src_type);
  free(src_config);

  free(dest_base);
  free(dest_cache);
  free(dest_type);
  free(dest_config);

  free(base);
  free(spec_file);

  return result;
}
