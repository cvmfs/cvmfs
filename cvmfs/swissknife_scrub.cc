/**
 * This file is part of the CernVM File System.
 */

#define __STDC_FORMAT_MACROS

#include "swissknife_scrub.h"

#include <string>


swissknife::ParameterList swissknife::CommandScrub::GetParams() {
  swissknife::ParameterList result;
  result.push_back(Parameter('r', "repository directory", false, false));
  // to be extended...
  return result;
}


int swissknife::CommandScrub::Main(const swissknife::ArgumentList &args) {
  std::string repo_path = *args.find('r')->second;

  return 0;
}
