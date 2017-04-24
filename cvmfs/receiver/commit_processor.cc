/**
 * This file is part of the CernVM File System.
 */

#include "commit_processor.h"

namespace receiver {

CommitProcessor::CommitProcessor() : num_errors_(0) {}

CommitProcessor::~CommitProcessor() {}

CommitProcessor::Result CommitProcessor::Process(
    const std::string& /*lease_path*/) {
  return kSuccess;
}

}  // namespace receiver