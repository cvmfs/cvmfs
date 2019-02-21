/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "publish/settings.h"

#include <string>
#include <vector>

#include "publish/except.h"
#include "util/string.h"

namespace publish {

void SettingsStorage::SetLocator(const std::string &locator) {
  std::vector<std::string> tokens = SplitString(locator, ',');
  if (tokens.size() != 3) {
    throw EPublish("malformed storage locator, expected format is "
                   "<type>,<temporary directory>,<endpoint>");
  }
  type_ = tokens[0];
  tmp_dir_ = tokens[1];
  endpoint_ = tokens[2];
}

}  // namespace publish
