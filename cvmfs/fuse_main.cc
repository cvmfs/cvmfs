/**
 * This file is part of the CernVM File System.
 *
 * The Fuse module entry point. Dynamically selects either the libfuse3
 * cvmfs fuse module or the libfuse2 one, depending on the availability and on
 * the mount options.
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


int main(int argc, char **argv) {
  // Getopt option parsing modifies globals and the argv vector
  const int opterr_save = opterr;
  const int optc = argc;
  assert(optc > 0);
  char **optv = reinterpret_cast<char **>(smalloc(optc * sizeof(char *)));
  for (int i = 0; i < optc; ++i) {
    optv[i] = strdup(argv[i]);
  }

  bool debug = false;
  unsigned enforce_libfuse = 0;
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

          if (HasPrefix(mount_options[i], "libfuse=", false /*ign_case*/)) {
            std::vector<std::string> t = SplitString(mount_options[i], '=');
            enforce_libfuse = String2Uint64(t[1]);
            if (debug) {
              LogCvmfs(kLogCvmfs, kLogDebug | kLogStdout,
                       "Debug: enforcing libfuse version %u", enforce_libfuse);
            }
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

  const std::string libname_fuse2 = platform_libname("cvmfs_fuse_stub");
  const std::string libname_fuse3 = platform_libname("cvmfs_fuse3_stub");

  std::string error_messages;

  if (enforce_libfuse > 0) {
    if ((enforce_libfuse < 2) || (enforce_libfuse > 3)) {
      LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr,
               "Error: invalid libfuse version '%u', valid values are 2 or 3.",
               enforce_libfuse);
      return 1;
    }
  }

  std::string local_lib_path = "./";
  if (getenv("CVMFS_LIBRARY_PATH") != NULL) {
    local_lib_path = getenv("CVMFS_LIBRARY_PATH");
    if (!local_lib_path.empty() && (*local_lib_path.rbegin() != '/'))
      local_lib_path.push_back('/');
  }

  // Try loading libfuse3 module, else fallback to version 2
  std::vector<std::string> library_paths;
  if ((enforce_libfuse == 0) || (enforce_libfuse == 3)) {
    library_paths.push_back(local_lib_path + libname_fuse3);
    library_paths.push_back("/usr/lib/" + libname_fuse3);
    library_paths.push_back("/usr/lib64/" + libname_fuse3);
#ifdef __APPLE__
    library_paths.push_back("/usr/local/lib/" + libname_fuse3);
#endif
  }
  if ((enforce_libfuse == 0) || (enforce_libfuse == 2)) {
    library_paths.push_back(local_lib_path + libname_fuse2);
    library_paths.push_back("/usr/lib/" + libname_fuse2);
    library_paths.push_back("/usr/lib64/" + libname_fuse2);
#ifdef __APPLE__
    library_paths.push_back("/usr/local/lib/" + libname_fuse2);
#endif
  }

  void *library_handle;
  std::vector<std::string>::const_iterator i = library_paths.begin();
  const std::vector<std::string>::const_iterator iend = library_paths.end();
  for (; i != iend; ++i) {
    library_handle = dlopen(i->c_str(), RTLD_NOW | RTLD_LOCAL);
    if (library_handle != NULL) {
      if (debug) {
        LogCvmfs(kLogCvmfs, kLogDebug | kLogStdout, "Debug: using library %s",
                 i->c_str());
      }
      break;
    }

    error_messages += std::string(dlerror()) + "\n";
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
