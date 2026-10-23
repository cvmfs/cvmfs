/**
 * This file is part of the CernVM File System.
 *
 * The Fuse module entry point. Loads the cvmfs fuse module (libcvmfs_fuse3),
 * in debug or normal flavour depending on the mount options.
 *
 */

#include "fuse_main.h"

#include <dlfcn.h>
#include <unistd.h>

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "util/logging.h"
#include "util/platform.h"
#include "util/smalloc.h"
#include "util/string.h"

using namespace stub;  // NOLINT


namespace {

std::vector<std::string> LibraryPaths(const std::string &libname) {
  std::string local_lib_path = "./";
  if (getenv("CVMFS_LIBRARY_PATH") != NULL) {
    local_lib_path = getenv("CVMFS_LIBRARY_PATH");
    if (!local_lib_path.empty() && (*local_lib_path.rbegin() != '/'))
      local_lib_path.push_back('/');
  }

  return {
      local_lib_path + libname,
      "/usr/lib/" + libname,
      "/usr/lib64/" + libname,
#ifdef __APPLE__
      // Since OS X El Capitan (10.11) came with SIP, we needed to relocate our
      // binaries from /usr/... to /usr/local/...
      "/usr/local/lib/" + libname,
#endif
  };
}

/**
 * Keeps an NFS mount answerable if its cvmfs2 process dies, see
 * nfs4/guardian.h.  Started by libcvmfs_nfsfuse as
 *   cvmfs2 __nfs_guardian__ <listen fd> <control fd> <mount point>
 */
int RunNfsGuardian(char **argv) {
  const std::vector<std::string> paths =
      LibraryPaths(platform_libname("cvmfs_nfsfuse"));
  for (const std::string &path : paths) {
    void *handle = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (handle == NULL)
      continue;
    typedef int (*GuardianMain)(int, int, const char *);
    GuardianMain guardian_main = reinterpret_cast<GuardianMain>(
        dlsym(handle, "cvmfs_nfsfuse_guardian_main"));
    if (guardian_main == NULL)
      return 1;
    return guardian_main(atoi(argv[2]), atoi(argv[3]), argv[4]);
  }
  return 1;
}

}  // anonymous namespace


int main(int argc, char **argv) {
  if ((argc == 5) && (strcmp(argv[1], "__nfs_guardian__") == 0))
    return RunNfsGuardian(argv);

  // Getopt option parsing modifies globals and the argv vector
  const int opterr_save = opterr;
  const int optc = argc;
  assert(optc > 0);
  char **optv = reinterpret_cast<char **>(smalloc(optc * sizeof(char *)));
  for (int i = 0; i < optc; ++i) {
    optv[i] = strdup(argv[i]);
  }

  bool debug = false;
  bool nfs = false;
  bool fskit = false;
  int c;
  opterr = 0;
  while ((c = getopt(optc, optv, "do:")) != -1) {
    switch (c) {
      case 'd':
        debug = true;
        break;
      case 'o':
        std::vector<std::string> mount_options = SplitString(optarg, ',');
        for (unsigned i = 0; i < mount_options.size(); ++i) {
          if (mount_options[i] == "debug") {
            debug = true;
          }
          if (mount_options[i] == "nfs") {
            nfs = true;
          }
          if (mount_options[i] == "obackend=fskit") {
            fskit = true;
          }
        }
        break;
    }
  }
  opterr = opterr_save;
  optind = 1;
  for (int i = 0; i < optc; ++i) {
    free(optv[i]);
  }
  free(optv);

  // -o nfs selects the frontend that mounts through the kernel's NFS client,
  // -o obackend=fskit the one built for the macFUSE FSKit backend
  const std::string libname_fuse3 = platform_libname(
      nfs ? "cvmfs_nfs_stub"
          : (fskit ? "cvmfs_fskit_stub" : "cvmfs_fuse3_stub"));

  std::string error_messages;

  const std::vector<std::string> library_paths = LibraryPaths(libname_fuse3);

  // dlopen the library at the first good path
  void *library_handle = nullptr;
  for (const std::string &path : library_paths) {
    library_handle = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (nullptr != library_handle) {
      if (debug) {
        LogCvmfs(kLogCvmfs, kLogDebug | kLogStdout, "Debug: using library %s",
                 path.c_str());
      }
      break;
    }
    error_messages += std::string(dlerror()) + "\n";
  }

  // Installations with only the NFS frontend still run helper commands
  // (e.g. __wpad__ from cvmfs_config) without -o nfs
  if (!library_handle && !nfs) {
    const std::vector<std::string> nfs_paths =
        LibraryPaths(platform_libname("cvmfs_nfs_stub"));
    for (const std::string &path : nfs_paths) {
      library_handle = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
      if (nullptr != library_handle)
        break;
    }
  }

  if (!library_handle) {
    LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr,
             "Error: failed to load cvmfs library, tried: '%s'\n%s",
             JoinStrings(library_paths, "' '").c_str(), error_messages.c_str());
    return 1;
  }

  CvmfsStubExports **exports_ptr = reinterpret_cast<CvmfsStubExports **>(
      dlsym(library_handle, "g_cvmfs_stub_exports"));
  if (exports_ptr == NULL) {
    LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr,
             "Error: symbol g_cvmfs_stub_exports not found");
    return 1;
  }

  return (*exports_ptr)->fn_main(argc, argv);
}
