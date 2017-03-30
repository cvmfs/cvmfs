/**
 * This file is part of the CernVM File System.
 */

#include <string>

#include "cvmfs_config.h"

#include "../logging.h"
#include "../swissknife.h"

swissknife::ParameterList MakeParameterList() {
  swissknife::ParameterList params;
  params.push_back(
      swissknife::Parameter::Mandatory('a', "Address of the gateway service"));

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
      LogCvmfs(kLogCvmfs, kLogStdout,
               "CVMFS gateway services receiver component. Usage:");
      for (size_t i = 0; i < params.size(); ++i) {
        LogCvmfs(kLogCvmfs, kLogStdout, "  \"%c\" - %s", params[i].key(),
                 params[i].description().c_str());
        return false;
      }
    }
  }

  for (size_t j = 0; j < params.size(); ++j) {
    if (!params[j].optional()) {
      if (arguments->find(params[j].key()) == arguments->end()) {
        LogCvmfs(kLogCvmfs, kLogStderr, "parameter -%c missing",
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

  return 0;
}
