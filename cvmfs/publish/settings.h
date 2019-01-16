/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_SETTINGS_H_
#define CVMFS_PUBLISH_SETTINGS_H_

#include <string>

namespace publish {

struct SettingsTransaction {
};

struct SettingsGc {
};

struct SettingsStorage {
};

struct SettingsRepository {
  std::string fqrn;
  std::string url;
};

}  // namespace publish

#endif  // CVMFS_PUBLISH_SETTINGS_H_
