/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_ROTATE_STATSDB_H_
#define CVMFS_SWISSKNIFE_ROTATE_STATSDB_H_

#include <string>

#include "swissknife.h"

namespace swissknife {

/**
 * Rotates the statistics database for a CernVM-FS repository.
 * Archives the current stats.db via VACUUM INTO, then clears it.
 * Uses the vendored SQLite to avoid depending on the system sqlite3 binary.
 */
class CommandRotateStatsDB : public Command {
 public:
  ~CommandRotateStatsDB() { }
  virtual std::string GetName() const { return "rotate-statsdb"; }
  virtual std::string GetDescription() const {
    return "Rotate (archive and clear) a CernVM-FS repository statistics "
           "database.\n"
           "Archives the database via VACUUM INTO and then deletes all rows "
           "from publish_statistics and gc_statistics tables.";
  }
  virtual ParameterList GetParams() const;
  int Main(const ArgumentList &args);
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_ROTATE_STATSDB_H_
