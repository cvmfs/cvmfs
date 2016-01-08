/**
 * This file is part of the CernVM File System.
 */

#include <string>

#include "cvmfs_config.h"

#include "logging.h"
#include "swissknife.h"

#include "swissknife_check.h"
#include "swissknife_gc.h"
#include "swissknife_graft.h"
#include "swissknife_hash.h"
#include "swissknife_history.h"
#include "swissknife_info.h"
#include "swissknife_letter.h"
#include "swissknife_lsrepo.h"
#include "swissknife_migrate.h"
#include "swissknife_pull.h"
#include "swissknife_scrub.h"
#include "swissknife_sign.h"
#include "swissknife_sync.h"
#include "swissknife_zpipe.h"


using namespace std;  // NOLINT

typedef vector<swissknife::Command *> Commands;
Commands command_list;

void Usage() {
  LogCvmfs(kLogCvmfs, kLogStdout,
    "CernVM-FS repository storage management commands\n"
    "Version %s\n"
    "Usage (normally called from cvmfs_server):\n"
    "  cvmfs_swissknife <command> [options]\n",
    VERSION);

  for (unsigned i = 0; i < command_list.size(); ++i) {
    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "\n"
             "Command %s\n"
             "--------", command_list[i]->GetName().c_str());
    for (unsigned j = 0; j < command_list[i]->GetName().length(); ++j) {
      LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "-");
    }
    LogCvmfs(kLogCvmfs, kLogStdout, "");
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
        LogCvmfs(kLogCvmfs, kLogStdout, "");
      }
    }  // Parameter list
  }  // Command list

  LogCvmfs(kLogCvmfs, kLogStdout, "");
}


int main(int argc, char **argv) {
  command_list.push_back(new swissknife::CommandCreate());
  command_list.push_back(new swissknife::CommandUpload());
  command_list.push_back(new swissknife::CommandRemove());
  command_list.push_back(new swissknife::CommandPeek());
  command_list.push_back(new swissknife::CommandSync());
  command_list.push_back(new swissknife::CommandApplyDirtab());
  command_list.push_back(new swissknife::CommandCreateTag());
  command_list.push_back(new swissknife::CommandRemoveTag());
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
  command_list.push_back(new swissknife::CommandHash());
  command_list.push_back(new swissknife::CommandInfo());
  command_list.push_back(new swissknife::CommandVersion());
  command_list.push_back(new swissknife::CommandMigrate());
  command_list.push_back(new swissknife::CommandScrub());
  command_list.push_back(new swissknife::CommandGc());

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
  int c;
  while ((c = getopt(argc, argv, option_string.c_str())) != -1) {
    bool valid_option = false;
    for (unsigned j = 0; j < params.size(); ++j) {
      if (c == params[j].key()) {
        valid_option = true;
        string *argument = NULL;
        if (!params[j].switch_only()) {
          argument = new string(optarg);
        }
        args[c] = argument;
        break;
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
  const int retval = command->Main(args);

  // delete the command list
        Commands::const_iterator i    = command_list.begin();
  const Commands::const_iterator iend = command_list.end();
  for (; i != iend; ++i) {
    delete *i;
  }
  command_list.clear();

  return retval;
}
