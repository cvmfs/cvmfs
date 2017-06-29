/**
 * This file is part of the CernVM File System.
 */

#include <string>

#include "cvmfs_config.h"

#include "../logging.h"
#include "../swissknife.h"

#include "reactor.h"

swissknife::ParameterList MakeParameterList() {
  swissknife::ParameterList params;
  params.push_back(
      swissknife::Parameter::Optional('i', "File descriptor to use for input"));
  params.push_back(swissknife::Parameter::Optional(
      'o', "File descriptor to use for output"));
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
        std::string* argument = NULL;
        if (!params[j].switch_only()) {
          argument = new std::string(optarg);
        }
        (*arguments)[c] = argument;
        break;
      }
    }

    if (!valid_option) {
      LogCvmfs(kLogReceiver, kLogStdout,
               "CVMFS gateway services receiver component. Usage:");
      for (size_t i = 0; i < params.size(); ++i) {
        LogCvmfs(kLogReceiver, kLogStdout, "  \"%c\" - %s", params[i].key(),
                 params[i].description().c_str());
        return false;
      }
    }
  }

  for (size_t j = 0; j < params.size(); ++j) {
    if (!params[j].optional()) {
      if (arguments->find(params[j].key()) == arguments->end()) {
        LogCvmfs(kLogReceiver, kLogStderr, "parameter -%c missing",
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

  LogSetCustomFile(0, "/var/log/cvmfs_receiver_out.log");
  LogSetCustomFile(1, "/var/log/cvmfs_receiver_err.log");

  int fdin = 0;
  int fdout = 1;
  if (arguments.find('i') != arguments.end()) {
    fdin = std::atoi(arguments.find('i')->second->c_str());
  }
  if (arguments.find('o') != arguments.end()) {
    fdout = std::atoi(arguments.find('o')->second->c_str());
  }
  receiver::Reactor reactor(fdin, fdout);

  if (!reactor.Run()) {
    LogCvmfs(kLogReceiver, kLogCustom1,
             "Error running CVMFS Receiver event loop");
    return 1;
  }

  return 0;
}
