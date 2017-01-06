/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "swissknife_diff.h"

using namespace std;  // NOLINT

namespace swissknife {

ParameterList CommandDiff::GetParams() const {
  swissknife::ParameterList r;
  r.push_back(Parameter::Mandatory('r', "repository directory / url"));
  return r;
}


int swissknife::CommandDiff::Main(const swissknife::ArgumentList &args) {
  return 0;
}

}  // namespace swissknife
