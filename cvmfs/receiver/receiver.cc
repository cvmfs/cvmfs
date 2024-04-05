/**
 * This file is part of the CernVM File System.
 */

#include <string>



#include "monitor.h"
#include "swissknife.h"
#include "util/exception.h"
#include "util/logging.h"
#include "util/posix.h"
#include "util/string.h"

#include "reactor.h"

static const char *kDefaultReceiverLogDir = "/var/log/cvmfs_receiver/";
static const char *kDefaultDebugLog = "/dev/null";

swissknife::ParameterList MakeParameterList() {
  swissknife::ParameterList params;
  params.push_back(
      swissknife::Parameter::Optional('i', "File descriptor to use for input"));
  params.push_back(swissknife::Parameter::Optional(
      'o', "File descriptor to use for output"));
  params.push_back(swissknife::Parameter::Optional(
      'w', "Watchdog stacktrace output dir, "
           "use without parameter to disable watchdog. "
           "Default: " + std::string(kDefaultReceiverLogDir)));
  params.push_back(swissknife::Parameter::Optional(
      'd', "Path to debug log. Ignored if the non-debug version runs, "
           "Default: " + std::string(kDefaultDebugLog)));
  return params;
}

bool ReadCmdLineArguments(int argc, char** argv,
                          const swissknife::ParameterList& params,
                          swissknife::ArgumentList* arguments) {
  // parse the command line arguments for the Command
  optind = 1;
  std::string option_string = "";

  for (unsigned j = 0; j < params.size(); ++j) {
    option_string.push_back(params[j].key());
    if (!params[j].switch_only()) option_string.push_back(':');
  }

  int c;
  while ((c = getopt(argc, argv, option_string.c_str())) != -1) {
    bool valid_option = false;
    for (unsigned j = 0; j < params.size(); ++j) {
      if (c == params[j].key()) {
        valid_option = true;
        (*arguments)[c].Reset();
        if (!params[j].switch_only()) {
          (*arguments)[c].Reset(new std::string(optarg));
        }
        break;
      }
    }

    if (!valid_option) {
      LogCvmfs(kLogReceiver, kLogSyslog,
               "CVMFS gateway services receiver component. Usage:");
      for (size_t i = 0; i < params.size(); ++i) {
        LogCvmfs(kLogReceiver, kLogSyslog, "  \"%c\" - %s", params[i].key(),
                 params[i].description().c_str());
      }
      return false;
    }
  }

  for (size_t j = 0; j < params.size(); ++j) {
    if (!params[j].optional()) {
      if (arguments->find(params[j].key()) == arguments->end()) {
        LogCvmfs(kLogReceiver, kLogSyslogErr, "parameter -%c missing",
                 params[j].key());
        return false;
      }
    }
  }

  return true;
}

int main(int argc, char** argv) {
  swissknife::ArgumentList arguments;
  if (!ReadCmdLineArguments(argc, argv, MakeParameterList(), &arguments)) {
    return 1;
  }

  // The receiver is spawned from the gateway go service. The gateway connects
  // a pipe to stdout and stderr but will collect the results only at the end.
  // This means that only small pieces of output can be send to stdout or
  // stderr, otherwise the pipes run full and block.
  // This should probably be addressed.
  //
  // For the debug mode, the gateway sets a debug log file that is collected
  // after the application quits.

  SetLogSyslogFacility(1);
  SetLogSyslogShowPID(true);

  int fdin = 0;
  int fdout = 1;
  std::string watchdog_out_dir = kDefaultReceiverLogDir;
  std::string debug_log = kDefaultDebugLog;
  if (arguments.find('i') != arguments.end()) {
    fdin = std::atoi(arguments.find('i')->second->c_str());
  }
  if (arguments.find('o') != arguments.end()) {
    fdout = std::atoi(arguments.find('o')->second->c_str());
  }
  if (arguments.find('w') != arguments.end()) {
    watchdog_out_dir = *arguments.find('w')->second;
  }
  if (arguments.find('d') != arguments.end()) {
    debug_log = *arguments.find('d')->second;
  }

  // The gateway hard-codes this to /var/log/cvmfs_receiver/debug.log
  SetLogDebugFile(debug_log);

  // Spawn monitoring process (watchdog)
  UniquePtr<Watchdog> watchdog;
  if (watchdog_out_dir != "") {
    if (!MkdirDeep(watchdog_out_dir, 0755)) {
      LogCvmfs(kLogReceiver, kLogSyslogErr | kLogStderr,
               "Failed to create stacktrace directory: %s",
               watchdog_out_dir.c_str());
      return 1;
    }
    std::string timestamp = GetGMTimestamp("%Y.%m.%d-%H.%M.%S");
    watchdog = Watchdog::Create(NULL);
    if (watchdog.IsValid() == false) {
      LogCvmfs(kLogReceiver, kLogSyslogErr | kLogStderr,
               "Failed to initialize watchdog");
      return 1;
    }
    watchdog->Spawn(watchdog_out_dir + "/stacktrace." + timestamp);
  }

  LogCvmfs(kLogReceiver, kLogSyslog, "CVMFS receiver started");

  receiver::Reactor reactor(fdin, fdout);

  try {
    if (!reactor.Run()) {
      LogCvmfs(kLogReceiver, kLogSyslogErr,
               "Error running CVMFS Receiver event loop");
      return 1;
    }
  } catch (const ECvmfsException& e) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "Runtime error during CVMFS Receiver event loop.\n"
             "%s",
             e.what());
    return 2;
  } catch (...) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "Unknown error during CVMFS Receiver event loop.\n");
      return 3;
  }

  LogCvmfs(kLogReceiver, kLogSyslog, "CVMFS receiver finished");

  return 0;
}
