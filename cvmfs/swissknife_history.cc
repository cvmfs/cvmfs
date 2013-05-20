/**
 * This file is part of the CernVM File System
 */

#include "cvmfs_config.h"
#include "swissknife_history.h"

using namespace std;  // NOLINT

int swissknife::CommandTag::Main(const swissknife::ArgumentList &args) {
  // Download & verify manifest

  // Compare base hash with hash in manifest (make sure we operate on the)
  // right history file

  // Download history database

  // Update trunk tag

  // Add / Remove tag to history database
  return 0;
}


int swissknife::CommandRollback::Main(const swissknife::ArgumentList &args) {
  // Download & verify manifest

  // Compare base hash with hash in manifest (make sure we operate on the
  // right history file)

  // Download history database

  // Safe highest revision from trunk tag

  // Remove all entries including destination catalog

  // Download rollback destination catalog

  // Update timestamp and revision

  // Commit catalog

  // Add new trunk tag / updated named tag to history
}
