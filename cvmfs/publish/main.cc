/**
 * This file is part of the CernVM File System.
 */


#include <algorithm>
#include <cstdlib>
#include <string>
#include <vector>

#include "publish/cmd_abort.h"
#include "publish/cmd_commit.h"
#include "publish/cmd_diff.h"
#include "publish/cmd_enter.h"
#include "publish/cmd_hash.h"
#include "publish/cmd_help.h"
#include "publish/cmd_info.h"
#include "publish/cmd_lsof.h"
#include "publish/cmd_mkfs.h"
#include "publish/cmd_transaction.h"
#include "publish/cmd_zpipe.h"
#include "publish/command.h"
#include "publish/except.h"
#include "util/logging.h"

using namespace std;  // NOLINT


static void PrintVersion() {
  LogCvmfs(kLogCvmfs, kLogStdout, "CernVM-FS Server Tool %s", CVMFS_VERSION);
}

static void Usage(const std::string &progname,
                  const publish::CommandList &clist) {
  LogCvmfs(kLogCvmfs, kLogStdout,
           "CernVM-FS Server Tool %s\n"
           "NOTE: This utility is for CernVM-FS internal use only for the time "
           "being!"
           "\n\n"
           "Usage:\n"
           "------\n"
           "  %s COMMAND [options] <parameters>\n\n"
           "Supported Commands\n"
           "-------------------\n",
           CVMFS_VERSION, progname.c_str());
  const vector<publish::Command *> commands = clist.commands();

  string::size_type max_len = 0;
  for (unsigned i = 0; i < commands.size(); ++i) {
    if (commands[i]->IsHidden())
      continue;
    max_len = std::max(commands[i]->GetName().length(), max_len);
  }

  for (unsigned i = 0; i < commands.size(); ++i) {
    if (commands[i]->IsHidden())
      continue;
    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "  %s",
             commands[i]->GetName().c_str());
    for (unsigned p = commands[i]->GetName().length(); p < max_len; ++p)
      LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, " ");
    LogCvmfs(kLogCvmfs, kLogStdout, "   %s", commands[i]->GetBrief().c_str());
  }

  LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "\n");
}


int main(int argc, char **argv) {
  publish::CommandList commands;
  commands.TakeCommand(new publish::CmdMkfs());
  commands.TakeCommand(new publish::CmdTransaction());
  commands.TakeCommand(new publish::CmdCommit());
  commands.TakeCommand(new publish::CmdAbort());
  commands.TakeCommand(new publish::CmdEnter());
  commands.TakeCommand(new publish::CmdInfo());
  commands.TakeCommand(new publish::CmdDiff());
  commands.TakeCommand(new publish::CmdHelp(&commands));
  commands.TakeCommand(new publish::CmdZpipe());
  commands.TakeCommand(new publish::CmdHash());
  commands.TakeCommand(new publish::CmdLsof());

  if (argc < 2) {
    Usage(argv[0], commands);
    return 1;
  }
  if ((string(argv[1]) == "--help") || (string(argv[1]) == "-h")) {
    Usage(argv[0], commands);
    return 0;
  }
  if ((string(argv[1]) == "--version") || (string(argv[1]) == "-v")) {
    PrintVersion();
    return 0;
  }

  publish::Command *command = commands.Find(argv[1]);
  if (command == NULL) {
    LogCvmfs(kLogCvmfs, kLogStderr, "unknown command: %s", argv[1]);
    Usage(argv[0], commands);
    return 1;
  }

  try {
    publish::Command::Options options = command->ParseOptions(argc, argv);
    return command->Main(options);
  } catch (const publish::EPublish &e) {
    if (e.failure() == publish::EPublish::kFailInvocation) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Invocation error: %s", e.msg().c_str());
    } else if (e.failure() == publish::EPublish::kFailMissingDependency) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Missing dependency: %s",
               e.msg().c_str());
    } else if (e.failure() == publish::EPublish::kFailPermission) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Permission error: %s", e.msg().c_str());
    } else {
      LogCvmfs(kLogCvmfs, kLogStderr, "(unexpected termination) %s", e.what());
    }
    return 1;
  }
}
