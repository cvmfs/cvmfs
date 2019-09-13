/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "cmd_help.h"

#include <algorithm>
#include <string>

#include "logging.h"
#include "publish/except.h"

using namespace std;  // NOLINT

namespace publish {

int CmdHelp::Main(const Options &options) {
  if (options.plain_args().size() != 1) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Usage: %s help %s",
             progname().c_str(), GetUsage().c_str());
    return 1;
  }

  Command *cmd = commands_->Find(options.plain_args()[0].value_str);
  if (cmd == NULL) {
    LogCvmfs(kLogCvmfs, kLogStderr, "No help for '%s'",
             options.plain_args()[0].value_str.c_str());
    return 1;
  }

  LogCvmfs(kLogCvmfs, kLogStdout, "\nHelp for '%s'", cmd->GetName().c_str());
  for (unsigned i = 0; i < cmd->GetName().length() + 11; ++i)
    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "=");
  LogCvmfs(kLogCvmfs, kLogStdout, "");
  LogCvmfs(kLogCvmfs, kLogStdout, "%s\n", cmd->GetDescription().c_str());

  LogCvmfs(kLogCvmfs, kLogStdout, "Usage:");
  LogCvmfs(kLogCvmfs, kLogStdout, "------");
  LogCvmfs(kLogCvmfs, kLogStdout, "  %s %s %s\n",
           progname().c_str(), cmd->GetName().c_str(), cmd->GetUsage().c_str());

  ParameterList params = cmd->GetParams();
  if (params.empty()) return 0;

  LogCvmfs(kLogCvmfs, kLogStdout, "Options:");
  LogCvmfs(kLogCvmfs, kLogStdout, "--------");
  string::size_type max_len = 0;
  for (unsigned i = 0; i < params.size(); ++i) {
    string::size_type l = params[i].key.length();
    if (!params[i].is_switch) {
      l += 3 + params[i].arg_name.length();
    }
    max_len = std::max(max_len, l);
  }
  for (unsigned i = 0; i < params.size(); ++i) {
    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "  -%c, --%s%s",
             params[i].short_key, params[i].key.c_str(),
             params[i].is_switch ?
               "" : (" <" + params[i].arg_name + ">").c_str());
    unsigned l = params[i].key.length();
    if (!params[i].is_switch) l += 3 + params[i].arg_name.length();
    for (unsigned p = l; p < max_len; ++p)
      LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, " ");
    LogCvmfs(kLogCvmfs, kLogStdout, "    %s%s",
             params[i].description.c_str(),
             params[i].is_optional ? "" : " [mandatory]");
  }
  LogCvmfs(kLogCvmfs, kLogStdout, "");

  return 0;
}

}  // namespace publish
