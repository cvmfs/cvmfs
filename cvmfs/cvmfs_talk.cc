/**
 * This file is part of the CernVM File System.
 *
 * cvmfs_talk runs query-response cycles against a running cvmfs instance.
 */

#include <errno.h>
#include <unistd.h>

#include <cassert>
#include <cstring>
#include <string>
#include <vector>

#include "logging.h"
#include "options.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/string.h"


struct InstanceInfo {
  bool IsDefined() {
    return !socket_path.empty() || !instance_name.empty();
  }

  // Called at most once by DeterminePath
  static std::string GetDefaultDomain() {
    std::string result;
    BashOptionsManager options_mgr;
    options_mgr.ParseDefault("");
    bool retval = options_mgr.GetValue("CVMFS_DEFAULT_DOMAIN", &result);
    if (!retval) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "Error: could not determin CVMFS_DEFAULT_DOMAIN");
    }
    return result;
  }

  bool DeterminePaths() {
    std::string fqrn = instance_name;
    if (fqrn.find('.') == std::string::npos) {
      static std::string default_domain = GetDefaultDomain();
      fqrn = fqrn + "." + default_domain;
    }

    BashOptionsManager options_mgr;
    options_mgr.ParseDefault(fqrn);
    if (!options_mgr.GetValue("CVMFS_WORKSPACE", &workspace)) {
      if (!options_mgr.GetValue("CVMFS_CACHE_DIR", &workspace)) {
        bool retval = options_mgr.GetValue("CVMFS_CACHE_BASE", &workspace);
        if (!retval) {
          LogCvmfs(kLogCvmfs, kLogStderr,
                   "CVMFS_WORKSPACE, CVMFS_CACHE_DIR, and CVMFS_CACHE_BASE "
                   "missing");
          return false;
        }

        std::string optarg;
        if (options_mgr.GetValue("CVMFS_SHARED_CACHE", &optarg) &&
            options_mgr.IsOn(optarg))
        {
          workspace += "/shared";
        } else {
          workspace += "/" + fqrn;
        }
      }
    }

    socket_path = workspace + "/cvmfs_io." + fqrn;
    return true;
  }

  bool CompleteInfo() {
    assert(IsDefined());

    if (socket_path.empty()) {
      bool retval = DeterminePaths();
      if (!retval)
        return false;
      identifier = "instance '" + instance_name + "' active in " + workspace;
    } else {
      workspace = GetParentPath(socket_path);
      identifier = "instance listening at " + socket_path;
    }
    return true;
  }

  std::string socket_path;
  std::string instance_name;
  std::string workspace;
  std::string identifier;
};


static bool ReadResponse(int fd) {
  std::string line;
  char buf;
  int retval;
  while ((retval = read(fd, &buf, 1)) == 1) {
    if (buf == '\n') {
      LogCvmfs(kLogCvmfs, kLogStdout, "%s", line.c_str());
      line.clear();
      continue;
    }
    line.push_back(buf);
  }
  return retval == 0;
}


bool SendCommand(const std::string &command, InstanceInfo instance_info) {
  bool retval = instance_info.CompleteInfo();
  if (!retval) return false;

  int fd = ConnectSocket(instance_info.socket_path);
  if (fd < 0) {
    if (errno == ENOENT) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "Seems like CernVM-FS is not running in %s (not found: %s)",
               instance_info.workspace.c_str(),
               instance_info.socket_path.c_str());
    } else {
      LogCvmfs(kLogCvmfs, kLogStderr, "Could not access %s (%d - %s)",
               instance_info.identifier.c_str(), errno, strerror(errno));
    }
    return false;
  }

  WritePipe(fd, command.data(), command.size());
  retval = ReadResponse(fd);
  close(fd);

  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Broken connection to %s (%d - %s)",
             instance_info.identifier.c_str(), errno, strerror(errno));
    return false;
  }
  return true;
}


static void Usage(const std::string &exe) {
  LogCvmfs(kLogCvmfs, kLogStdout,
    "Usage: %s [-i instance | -p socket] <command>                     \n"
    "   By default, iteratate through all instances.                   \n"
    "\n"
    "Example:                                                          \n"
    "  %s -i atlas.cern.ch pid                                         \n"
    "\n"
    "Commands:                                                         \n"
    "  tracebuffer flush      flushes the trace buffer to disk         \n"
    "  cache instance         describes the active cache manager       \n"
    "  cache size             gets current size of file cache          \n"
    "  cache list             gets files in cache                      \n"
    "  cache list pinned      gets pinned file catalogs in cache       \n"
    "  cache list catalogs    gets all file catalogs in cache          \n"
    "  cleanup <MB>           cleans file cache until size <= <MB>     \n"
    "  cleanup rate <period>  n.o. cleanups in the last <period> min   \n"
    "  evict <path>           removes <path> from the cache            \n"
    "  pin <path>             pins <path> in the cache                 \n"
    "  mountpoint             returns the mount point                  \n"
    "  device id              returns major:minor virtual device id    \n"
    "                         on Linux and 0:0 on macOS                \n"
    "  remount [sync]         look for new catalogs                    \n"
    "  revision               gets the repository revision             \n"
    "  max ttl info           gets the maximum ttl                     \n"
    "  max ttl set <minutes>  sets the maximum ttl                     \n"
    "  nameserver get         get the DNS server                       \n"
    "  nameserver set <host>  sets a DNS server                        \n"
    "  host info              get host chain and their rtt,            \n"
    "                         if already probed                        \n"
    "  host probe             orders the host chain according to rtt   \n"
    "  host probe geo         let Stratum 1s order the host chain and  \n"
    "                         fallback proxies using the Geo-API       \n"
    "  host switch            switches to the next host in the chain   \n"
    "  host set <host list>   sets a new host chain                    \n"
    "  proxy info             gets load-balance proxy groups           \n"
    "  proxy rebalance        randomly selects a new proxy server      \n"
    "                         from the current load-balance group      \n"
    "  proxy group switch     switches to the next load-balance        \n"
    "                         proxy group in the chain                 \n"
    "  proxy set <proxy list> sets a new chain of load-balance proxy   \n"
    "                         groups (not including fallback proxies)  \n"
    "  proxy fallback <list>  sets a new list of fallback proxies      \n"
    "  external host info     gets info about external host chain      \n"
    "  external host switch   switches to the next external host       \n"
    "  external host set                                               \n"
    "       <host list>       sets external host chain                 \n"
    "  external proxy info    gets info about external proxy groups    \n"
    "  external proxy set                                              \n"
    "       <proxy list>      sets chain of external proxy groups      \n"
    "  timeout info           gets the network timeouts                \n"
    "  timeout set                                                     \n"
    "       <proxy> <direct>  sets the network timeouts in seconds     \n"
    "  pid                    gets the pid                             \n"
    "  pid cachemgr           gets the pid of the shared cache manager \n"
    "  pid watchdog           gets the pid of the crash handler process\n"
    "  parameters             dumps the effective parameters           \n"
    "  reset error counters   resets the counter for I/O errors        \n"
    "  hotpatch history       shows timestamps and version info of     \n"
    "                         loaded (hotpatched) Fuse modules         \n"
    "  version                gets cvmfs version                       \n"
    "  version patchlevel     gets cvmfs patchlevel                    \n"
    "  open catalogs          shows information about currently        \n"
    "                         loaded catalogs (_not_ all cached ones)  \n"
    "  latency                show the latencies of different fuse     \n"
    "                         calls (requires CVMFS_INSTRUMENT_FUSE)   \n"
    "\n",
    exe.c_str(), exe.c_str());
}


int main(int argc, char *argv[]) {
  InstanceInfo instance_info;
  std::string command;

  int c;
  // 's' for socket would have been a better option letter but we keep 'p'
  // for backwards compatibility.  The '+' at the beginning of the option stirng
  // prevents permutation of the option and non-option arguments.
  while ((c = getopt(argc, argv, "+hi:p:")) != -1) {
    switch (c) {
      case 'h':
        Usage(argv[0]);
        return 0;
      case 'p':
        instance_info.socket_path = optarg;
        break;
      case 'i':
        instance_info.instance_name = optarg;
        break;
      case '?':
      default:
        Usage(argv[0]);
        return 1;
    }
  }

  for (; optind < argc; ++optind) {
    command += argv[optind];
    if (optind < (argc - 1))
      command.push_back(' ');
  }
  if (command.empty()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Command missing");
    return 1;
  }

  int retcode = 0;
  if (!instance_info.IsDefined()) {
    BashOptionsManager options_mgr;
    options_mgr.ParseDefault("");
    std::string opt_repos;
    options_mgr.GetValue("CVMFS_REPOSITORIES", &opt_repos);
    std::vector<std::string> repos = SplitString(opt_repos, ',');
    for (unsigned i = 0; i < repos.size(); ++i) {
      if (repos[i].empty())
        continue;
      instance_info.instance_name = repos[i];
      LogCvmfs(kLogCvmfs, kLogStdout, "%s:", repos[i].c_str());
      bool retval = SendCommand(command, instance_info);
      if (!retval) retcode = 1;
    }
  } else {
    bool retval = SendCommand(command, instance_info);
    if (!retval) retcode = 1;
  }
  return retcode;
}
