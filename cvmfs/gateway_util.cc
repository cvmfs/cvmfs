/**
 * This file is part of the CernVM File System.
 */

#include "gateway_util.h"

#include <vector>

#include "logging.h"
#include "util/string.h"

namespace gateway {

int APIVersion() { return 1; }

bool ReadKeys(const std::string& key_file_name, std::string* key_id,
              std::string* secret) {
  if (!(key_id && secret)) {
    return false;
  }

  FILE* key_file_fd = std::fopen(key_file_name.c_str(), "r");
  if (!key_file_fd) {
    return false;
  }

  std::string line;
  if (!GetLineFile(key_file_fd, &line)) {
    return false;
    fclose(key_file_fd);
  }

  fclose(key_file_fd);

  std::vector<std::string> tokens = SplitString(line, ' ');
  if (tokens.size() < 2) {
    return false;
  }

  if (tokens[0] == "test") {
    *key_id = tokens[1];
    *secret = tokens[2];
  } else {
    return false;
  }

  return true;
}

}  // namespace gateway
