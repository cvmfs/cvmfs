/**
 * This file is part of the CernVM File System
 *
 * This tool acts as an entry point for all the server-related
 * cvmfs tasks, such as uploading files and checking the sanity of
 * a repository.
 */

#include "cvmfs_config.h"
#include "swissknife.h"

#include <unistd.h>

#include <vector>

#include "logging.h"
#include "download.h"
#include "signature.h"
#include "swissknife_zpipe.h"
#include "swissknife_check.h"
#include "swissknife_lsrepo.h"
#include "swissknife_pull.h"
#include "swissknife_sign.h"
#include "swissknife_letter.h"
#include "swissknife_sync.h"
#include "swissknife_info.h"
#include "swissknife_history.h"
#include "swissknife_migrate.h"
#include "swissknife_scrub.h"

using namespace std;  // NOLINT
using namespace swissknife;

vector<swissknife::Command *> command_list;
download::DownloadManager *swissknife::g_download_manager;
signature::SignatureManager *swissknife::g_signature_manager;

void swissknife::Usage() {
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
  g_download_manager = new download::DownloadManager();
  g_signature_manager = new signature::SignatureManager();

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
  command_list.push_back(new swissknife::CommandRollback());
  command_list.push_back(new swissknife::CommandRollbackTag());
  command_list.push_back(new swissknife::CommandSign());
  command_list.push_back(new swissknife::CommandLetter());
  command_list.push_back(new swissknife::CommandCheck());
  command_list.push_back(new swissknife::CommandListCatalogs());
  command_list.push_back(new swissknife::CommandPull());
  command_list.push_back(new swissknife::CommandZpipe());
  command_list.push_back(new swissknife::CommandInfo());
  command_list.push_back(new swissknife::CommandVersion());
  command_list.push_back(new swissknife::CommandMigrate());
  command_list.push_back(new swissknife::CommandScrub());

  if (argc < 2) {
    swissknife::Usage();
    return 1;
  }
  if ((string(argv[1]) == "--help")) {
    swissknife::Usage();
    return 0;
  }
  if ((string(argv[1]) == "--version")) {
    swissknife::CommandVersion().Main(swissknife::ArgumentList());
    return 0;
  }

  for (unsigned i = 0; i < command_list.size(); ++i) {
    if (command_list[i]->GetName() == string(argv[1])) {
      swissknife::ArgumentList args;
      optind = 1;
      string option_string = "";
      swissknife::ParameterList params = command_list[i]->GetParams();
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
          swissknife::Usage();
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
      return command_list[i]->Main(args);
    }
  }

  delete g_signature_manager;
  delete g_download_manager;
  swissknife::Usage();
  return 1;
}
