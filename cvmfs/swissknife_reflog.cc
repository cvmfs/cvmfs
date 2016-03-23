/**
 * This file is part of the CernVM File System.
 */

#include "swissknife_reflog.h"

namespace swissknife {

CommandBootstrapReflog::CommandBootstrapReflog() {

}


CommandBootstrapReflog::~CommandBootstrapReflog() {

}


ParameterList CommandBootstrapReflog::GetParams() {
  return ParameterList();
}


int CommandBootstrapReflog::Main(const ArgumentList &args) {
  return 0;
}

}  // namespace swissknife
