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
#include "swissknife_sync.h"
#include "swissknife_info.h"
#include "swissknife_history.h"
#include "swissknife_migrate.h"
#include "swissknife_scrub.h"

using namespace std;  // NOLINT
using namespace swissknife;

download::DownloadManager *swissknife::g_download_manager;
signature::SignatureManager *swissknife::g_signature_manager;

void swissknife::Usage() {
  LogCvmfs(kLogCvmfs, kLogStdout,
    "CernVM-FS repository storage management commands\n"
    "Version %s\n"
    "Usage (normally called from cvmfs_server):\n"
    "  cvmfs_swissknife <command> [options]\n",
    VERSION);

  AbstractCommand::IntrospectionData info = AbstractCommand::Introspect();
  AbstractCommand::IntrospectionData::const_iterator i    = info.begin();
  AbstractCommand::IntrospectionData::const_iterator iend = info.end();
  for (; i != iend; ++i) {
    const CommandIntrospection &command_info = *i;

    // headline
    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "\n"
             "Command %s\n"
             "--------", command_info.name.c_str());
    for (unsigned j = 0; j < command_info.name.length(); ++j) {
      LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "-");
    }

    // description
    LogCvmfs(kLogCvmfs, kLogStdout, "\n%s\n",
             command_info.description.c_str());

    // parameters
    const ParameterList &params = command_info.parameters;
    if (params.empty()) {
      continue;
    }
    LogCvmfs(kLogCvmfs, kLogStdout, "Options:");
    ParameterList::const_iterator j    = params.begin();
    ParameterList::const_iterator jend = params.end();
    for (; j != jend; ++j) {
      LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "  -%c    %s",
               j->key(), j->description().c_str());
      if (j->optional())
        LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, " (optional)");
      LogCvmfs(kLogCvmfs, kLogStdout, "");
    }
  }

  LogCvmfs(kLogCvmfs, kLogStdout, "");
}


void AbstractCommand::RegisterPlugins() {
  RegisterPlugin<swissknife::CommandCreate>();
  RegisterPlugin<swissknife::CommandUpload>();
  RegisterPlugin<swissknife::CommandRemove>();
  RegisterPlugin<swissknife::CommandPeek>();
  RegisterPlugin<swissknife::CommandSync>();
  RegisterPlugin<swissknife::CommandTag>();
  RegisterPlugin<swissknife::CommandRollback>();
  RegisterPlugin<swissknife::CommandSign>();
  RegisterPlugin<swissknife::CommandCheck>();
  RegisterPlugin<swissknife::CommandListCatalogs>();
  RegisterPlugin<swissknife::CommandPull>();
  RegisterPlugin<swissknife::CommandZpipe>();
  RegisterPlugin<swissknife::CommandInfo>();
  RegisterPlugin<swissknife::CommandVersion>();
  RegisterPlugin<swissknife::CommandMigrate>();
  RegisterPlugin<swissknife::CommandScrub>();
}


ArgumentList AbstractCommand::ParseArguments(int argc, char **argv,
                                             ParameterList &params) const
{
  ArgumentList args;
  bool found_help_switch = false;

  optind = 1;
  std::string option_string = "";
  ParameterList::const_iterator i    = params.begin();
  ParameterList::const_iterator iend = params.end();

  // define `getopt` option string
  for (; i != iend; ++i) {
    option_string.push_back(i->key());
    if (! i->switch_only()) {
      option_string.push_back(':');
    }
  }

  // parse command line parameters
  int c;
  while ((c = getopt(argc, argv, option_string.c_str())) != -1) {
    bool valid_option = false;

    i = params.begin();
    for (; i != iend; ++i) {
      if (c == i->key()) {
        valid_option = true;
        std::string *argument = NULL;
        if (! i->switch_only()) {
          argument = new std::string(optarg);
        }
        args[c] = argument;

        if (i->help_switch()) {
          found_help_switch = true;
        }

        break;
      }
    }

    if (!valid_option) {
      swissknife::Usage();
      exit(1);
    }
  }

  // check if all mandatory parameters are found
  // (Note: the help switch might override all other mandatory options)
  if (! found_help_switch) {
    i = params.begin();
    for (; i != iend; ++i) {
      if (i->mandatory() && args.find(i->key()) == args.end()) {
        LogCvmfs(kLogCvmfs, kLogStderr, "parameter -%c missing",
                 i->key());
        exit(1);
      }
    }
  }

  return args;
}


int main(int argc, char **argv) {
  g_download_manager = new download::DownloadManager();
  g_signature_manager = new signature::SignatureManager();

  if (argc < 2) {
    swissknife::Usage();
    return 1;
  }
  if ((string(argv[1]) == "--help")) {
    swissknife::Usage();
    return 0;
  }
  if ((string(argv[1]) == "--version")) {
    AbstractCommand::Construct("version")->Run(swissknife::ArgumentList());
    return 0;
  }

  AbstractCommand *command = AbstractCommand::Construct(argv[1]);
  if (command == NULL) {
    swissknife::Usage();
    exit(1);
  }

  int exit_code = command->Main(argc, argv);

  delete g_signature_manager;
  delete g_download_manager;

  return exit_code;
}
