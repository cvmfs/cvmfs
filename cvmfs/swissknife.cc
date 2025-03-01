/**
 * This file is part of the CernVM File System
 *
 * This tool acts as an entry point for all the server-related
 * cvmfs tasks, such as uploading files and checking the sanity of
 * a repository.
 */

#include "swissknife.h"


#include <unistd.h>

#include <cassert>
#include <vector>

#include "manifest.h"
#include "manifest_fetch.h"
#include "util/logging.h"

using namespace std;  // NOLINT

namespace swissknife {

Command::Command() {}

Command::~Command() {}

}  // namespace swissknife
