/**
 * This file is part of the CernVM File System.
 */

#include "loader_talk.h"

#include "logging.h"

using namespace std;  // NOLINT

namespace loader {

bool Init(const string &socket_path) {
  
  return false;
}


void Spawn() {
}


void Fini() {
}


/**
 * Connects to a loader socket and triggers the reload
 */
int MainReload(const std::string &socket_path) {
  return 1;
}
  
}  // namespace loader
