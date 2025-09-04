/**
 * This file is part of the CernVM File System.
 */

#include <cassert>
#include <string>

#include "statistics_database.h"
#include "swissknife.h"
#include "swissknife_check.h"
#include "swissknife_filestats.h"
#include "swissknife_gc.h"
#include "swissknife_graft.h"
#include "swissknife_history.h"
#include "swissknife_info.h"
#include "swissknife_ingest.h"
#include "swissknife_ingestsql.h"
#include "swissknife_lease.h"
#include "swissknife_letter.h"
#include "swissknife_list_reflog.h"
#include "swissknife_lsrepo.h"
#include "swissknife_migrate.h"
#include "swissknife_notify.h"
#include "swissknife_pull.h"
#include "swissknife_reflog.h"
#include "swissknife_scrub.h"
#include "swissknife_sign.h"
#include "swissknife_sync.h"
#include "swissknife_zpipe.h"
#include "util/logging.h"
#include "util/posix.h"
#include "util/string.h"

using namespace std;  // NOLINT

typedef vector<swissknife::Command *> Commands;
Commands command_list;

void Usage() {
  LogCvmfs(kLogCvmfs, kLogStdout,
           "CernVM-FS repository storage management commands\n"
           "Usage (normally called from cvmfs_server):\n"
           "  cvmfs_swissknife <command> [options]\n");

  for (unsigned i = 0; i < command_list.size(); ++i) {
    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
             "\n"
             "Command %s\n"
             "--",
             command_list[i]->GetName().c_str());
    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "\n");
    LogCvmfs(kLogCvmfs, kLogStdout, "%s",
             command_list[i]->GetDescription().c_str());
    swissknife::ParameterList params = command_list[i]->GetParams();
    if (!params.empty()) {
      LogCvmfs(kLogCvmfs, kLogStdout, "Options:");
      for (unsigned j = 0; j < params.size(); ++j) {
        LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "  -%c    %s",
                 params[j].key(), params[j].description().c_str());
        if (params[j].optional())
          LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, " (optional)");
        LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "\n");
      }
    }  // Parameter list
  }  // Command list

  LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "\n");
}


int main(int argc, char **argv) {
  // Set default logging facilities
  DefaultLogging::Set(kLogStdout, kLogStderr);

  command_list.push_back(new swissknife::CommandCreate());
  command_list.push_back(new swissknife::CommandUpload());
  command_list.push_back(new swissknife::CommandRemove());
  command_list.push_back(new swissknife::CommandPeek());
  command_list.push_back(new swissknife::CommandSync());
  command_list.push_back(new swissknife::CommandApplyDirtab());
  command_list.push_back(new swissknife::CommandEditTag());
  command_list.push_back(new swissknife::CommandListTags());
  command_list.push_back(new swissknife::CommandInfoTag());
  command_list.push_back(new swissknife::CommandRollbackTag());
  command_list.push_back(new swissknife::CommandEmptyRecycleBin());
  command_list.push_back(new swissknife::CommandSign());
  command_list.push_back(new swissknife::CommandLetter());
  command_list.push_back(new swissknife::CommandCheck());
  command_list.push_back(new swissknife::CommandListCatalogs());
  command_list.push_back(new swissknife::CommandPull());
  command_list.push_back(new swissknife::CommandZpipe());
  command_list.push_back(new swissknife::CommandGraft());
  command_list.push_back(new swissknife::CommandInfo());
  command_list.push_back(new swissknife::CommandVersion());
  command_list.push_back(new swissknife::CommandMigrate());
  command_list.push_back(new swissknife::CommandScrub());
  command_list.push_back(new swissknife::CommandGc());
  command_list.push_back(new swissknife::CommandListReflog());
  command_list.push_back(new swissknife::CommandReconstructReflog());
  command_list.push_back(new swissknife::CommandLease());
  command_list.push_back(new swissknife::Ingest());
  command_list.push_back(new swissknife::IngestSQL());
  command_list.push_back(new swissknife::CommandNotify());
  command_list.push_back(new swissknife::CommandFileStats());

  if (argc < 2) {
    Usage();
    return 1;
  }
  if ((string(argv[1]) == "--help")) {
    Usage();
    return 0;
  }
  if ((string(argv[1]) == "--version")) {
    swissknife::CommandVersion().Main(swissknife::ArgumentList());
    return 0;
  }

  // find the command to be run
  swissknife::Command *command = NULL;
  for (unsigned i = 0; i < command_list.size(); ++i) {
    if (command_list[i]->GetName() == string(argv[1])) {
      command = command_list[i];
      break;
    }
  }

  if (NULL == command) {
    Usage();
    return 1;
  }

  bool display_statistics = false;

  // parse the command line arguments for the Command
  swissknife::ArgumentList args;
  optind = 1;
  string option_string = "";
  swissknife::ParameterList params = command->GetParams();
  for (unsigned j = 0; j < params.size(); ++j) {
    option_string.push_back(params[j].key());
    if (!params[j].switch_only())
      option_string.push_back(':');
  }
  // Now adding the generic -+ extra option command
  option_string.push_back(swissknife::Command::kGenericParam);
  option_string.push_back(':');
  int c;
  while ((c = getopt(argc, argv, option_string.c_str())) != -1) {
    bool valid_option = false;
    for (unsigned j = 0; j < params.size(); ++j) {
      if (c == params[j].key()) {
        assert(c != swissknife::Command::kGenericParam);
        valid_option = true;
        args[c].Reset();
        if (!params[j].switch_only()) {
          args[c].Reset(new string(optarg));
        }
        break;
      }
    }
    if (c == swissknife::Command::kGenericParam) {
      valid_option = true;
      vector<string> flags = SplitString(
          optarg, swissknife::Command::kGenericParamSeparator);
      for (unsigned i = 0; i < flags.size(); ++i) {
        if (flags[i] == "stats") {
          display_statistics = true;
        }
      }
    }
    if (!valid_option) {
      Usage();
      return 1;
    }
  }
  for (unsigned j = 0; j < params.size(); ++j) {
    if (!params[j].optional()) {
      if (args.find(params[j].key()) == args.end()) {
        LogCvmfs(kLogCvmfs, kLogStderr, "parameter -%c missing",
                 params[j].key());
        return 1;
      }
    }
  }

  // run the command
  const string start_time = GetGMTimestamp();
  const int retval = command->Main(args);
  const string finish_time = GetGMTimestamp();

  if (display_statistics) {
    LogCvmfs(kLogCvmfs, kLogStdout, "Command statistics");
    LogCvmfs(kLogCvmfs, kLogStdout, "%s",
             command->statistics()
                 ->PrintList(perf::Statistics::kPrintHeader)
                 .c_str());
  }

  // delete the command list
  Commands::const_iterator i = command_list.begin();
  const Commands::const_iterator iend = command_list.end();
  for (; i != iend; ++i) {
    delete *i;
  }
  command_list.clear();

  return retval;
}
