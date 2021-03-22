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

#include "cvmfs_config.h"
#include "logging.h"
#include "shrinkwrap/fs_traversal.h"
#include "shrinkwrap/fs_traversal_interface.h"
#include "util/string.h"
#include "util_concurrency.h"


namespace {

struct Params {
  bool CheckType(const std::string &type) {
    if ((type == "cvmfs") || (type == "posix")) return true;
    LogCvmfs(kLogCvmfs, kLogStderr, "Unknown type: %s", type.c_str());
    return false;
  }

  bool Check() {
    if (!CheckType(src_type) || !CheckType(dst_type)) return false;

    return true;
  }

  void Complete() {
    if (dst_data_dir.empty())
      dst_data_dir = dst_base_dir + "/.data";

    if (spec_trace_path.empty())
      spec_trace_path = repo_name + ".spec";

    if (num_parallel == 0)
      num_parallel = 2 * GetNumberOfCpuCores();
  }

  Params()
    : repo_name()
    , src_type("cvmfs")
    , src_base_dir("/cvmfs")
    , src_config_path("cvmfs.conf:cvmfs.local")
    , src_data_dir()
    , dst_type("posix")
    , dst_config_path()
    , dst_base_dir("/export/cvmfs")
    , dst_data_dir()
    , spec_trace_path()
    , num_parallel(0)
    , stat_period(10)
    , do_garbage_collection(false)
  { }

  std::string repo_name;
  std::string src_type;
  std::string src_base_dir;
  std::string src_config_path;
  std::string src_data_dir;
  std::string dst_type;
  std::string dst_config_path;
  std::string dst_base_dir;
  std::string dst_data_dir;
  std::string spec_trace_path;
  uint64_t num_parallel;
  uint64_t stat_period;
  bool do_garbage_collection;
};

void Usage() {
  LogCvmfs(kLogCvmfs, kLogStdout,
       "CernVM File System Shrinkwrapper, version %s\n\n"
       "This tool takes a cvmfs repository and outputs\n"
       "to a destination files system.\n"
       "Usage: cvmfs_shrinkwrap "
       "[-r][-sbcf][-dxyz][-t][-jrg]\n"
        "Options:\n"
        " -r --repo        Repository name [required]\n"
        " -s --src-type    Source filesystem type [default:cvmfs]\n"
        " -b --src-base    Source base location [default:/cvmfs/]\n"
        " -c --src-cache   Source cache\n"
        " -f --src-config  Source config [default:cvmfs.conf:cvmfs.local]\n"
        " -d --dest-type   Dest filesystem type [default:posix]\n"
        " -x --dest-base   Dest base [default:/export/cvmfs]\n"
        " -y --dest-cache  Dest cache [default:$BASE/.data]\n"
        " -z --dest-config Dest config\n"
        " -t --spec-file   Specification file [default=$REPO.spec]\n"
        " -j --threads     Number of concurrent copy threads [default:2*CPUs]\n"
        " -p --stat-period Frequency of stat prints, 0 disables [default:10]\n"
        " -g --gc          Perform garbage collection on destination\n",
           VERSION);
}

}  // anonymous namespace

int main(int argc, char **argv) {
  Params params;

  int c;
  static struct option long_opts[] = {
      /* All of the options require an argument */
      {"help",        no_argument, 0, 'h'},
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
      {"stat-period", required_argument, 0, 'p'},
      {"gc",          no_argument, 0, 'g'},
      {0, 0, 0, 0}
    };

  static const char short_opts[] = "hb:s:r:c:f:d:x:y:t:j:p:g";

  while ((c = getopt_long(argc, argv, short_opts, long_opts, NULL)) >= 0) {
    switch (c) {
      case 'h':
        Usage();
        return 0;
      case 'r':
        params.repo_name = optarg;
        break;
      case 's':
        params.src_type = optarg;
        break;
      case 'b':
        params.src_base_dir = optarg;
        break;
      case 'c':
        params.src_data_dir = optarg;
        break;
      case 'f':
        params.src_config_path = optarg;
        break;
      case 'd':
        params.dst_type = optarg;
        break;
      case 'x':
        params.dst_base_dir = optarg;
        break;
      case 'y':
        params.dst_data_dir = optarg;
        break;
      case 'z':
        params.dst_config_path = optarg;
        break;
      case 't':
        params.spec_trace_path = optarg;
        break;
      case 'j':
        if (!String2Uint64Parse(optarg, &params.num_parallel)) {
          LogCvmfs(kLogCvmfs, kLogStderr,
            "Invalid value passed to 'j': %s : only non-negative integers",
             optarg);
          Usage();
          return 1;
        }
        break;
      case 'g':
        params.do_garbage_collection = true;
        break;
      case 'p':
        if (!String2Uint64Parse(optarg, &params.stat_period)) {
          LogCvmfs(kLogCvmfs, kLogStderr,
            "Invalid value passed to 'p': %s : only non-negative integers",
             optarg);
          Usage();
          return 1;
        }
        break;
      case '?':
      default:
        Usage();
        return 1;
    }
  }

  if (!params.Check()) return 1;
  params.Complete();

  struct fs_traversal *src = shrinkwrap::FindInterface(params.src_type.c_str());
  if (!src) {
    return 1;
  }
  src->context_ = src->initialize(
    params.repo_name.c_str(),
    params.src_base_dir.c_str(),
    params.src_data_dir.c_str(),
    params.src_config_path.c_str(),
    params.num_parallel);
  if (!src->context_) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Unable to initialize source");
    return 1;
  }

  struct fs_traversal *dest =
    shrinkwrap::FindInterface(params.dst_type.c_str());
  if (!dest) {
    return 1;
  }
  dest->context_ = dest->initialize(
    params.repo_name.c_str(),
    params.dst_base_dir.c_str(),
    params.dst_data_dir.c_str(),
    params.dst_config_path.c_str(),
    params.num_parallel);
  if (!dest->context_) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Unable to initialize destination");
    src->finalize(src->context_);
    delete src;
    return 1;
  }

  dest->archive_provenance(src->context_, dest->context_);

  int result = shrinkwrap::SyncInit(
    src,
    dest,
    "", /* spec_base_dir, unused */
    params.spec_trace_path.c_str(),
    params.num_parallel,
    params.stat_period);

  src->finalize(src->context_);
  if (params.do_garbage_collection) {
    shrinkwrap::GarbageCollect(dest);
  }
  dest->finalize(dest->context_);

  delete src;
  delete dest;

  return result;
}
