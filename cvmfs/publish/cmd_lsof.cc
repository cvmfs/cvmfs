/**
 * This file is part of the CernVM File System.
 */


#include "cmd_lsof.h"

#include <string>
#include <vector>

#include "util/logging.h"
#include "util/posix.h"

int publish::CmdLsof::Main(const Options &options) {
  std::string path = options.plain_args()[0].value_str;
  std::vector<LsofEntry> entries = Lsof(path);
  for (unsigned i = 0; i < entries.size(); ++i) {
    LogCvmfs(kLogCvmfs, kLogStdout, "%s %s", entries[i].read_only ? "ro" : "rw",
             entries[i].path.c_str());
  }
  return 0;
}
