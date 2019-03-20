/**
 * This file is part of the CernVM File System.
 */

#include "gateway_util.h"

#include <fcntl.h>

#include <algorithm>
#include <vector>

#include "logging.h"
#include "util/posix.h"
#include "util/string.h"


namespace gateway {

int APIVersion() { return 2; }

bool ReadKeys(const std::string& key_file_name, std::string* key_id,
              std::string* secret) {
  if (!(key_id && secret)) {
    return false;
  }

  int key_file_fd = open(key_file_name.c_str(), O_RDONLY);
  if (!key_file_fd) {
    return false;
  }

  std::string body;
  if (!SafeReadToString(key_file_fd, &body)) {
    close(key_file_fd);
    return false;
  }

  close(key_file_fd);

  return ParseKey(body, key_id, secret);
}

bool ParseKey(const std::string& body, std::string* key_id,
              std::string* secret) {
  std::string line = GetLineMem(body.data(), body.size());
  std::string l = Trim(ReplaceAll(line, "\t", " "));
  l.erase(std::unique(l.begin(), l.end()), l.end());
  std::vector<std::string> tokens = SplitString(l, ' ');

  if (tokens.size() < 2 || tokens.size() > 3) {
    return false;
  }

  if (tokens[0] == "plain_text") {
    *key_id = tokens[1];
    *secret = tokens[2];
  } else {
    return false;
  }

  return true;
}

}  // namespace gateway
