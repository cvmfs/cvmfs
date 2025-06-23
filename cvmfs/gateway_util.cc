/**
 * This file is part of the CernVM File System.
 */

#include "gateway_util.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <vector>

#include "util/logging.h"
#include "util/posix.h"
#include "util/string.h"

namespace {

bool BothAreSpaces(const char &c1, const char &c2) {
  return c1 == ' ' && (c1 == c2);
}

}  // namespace

namespace gateway {

int APIVersion() { return 2; }

GatewayKey ReadGatewayKey(const std::string &key_file_name) {
  std::string id;
  std::string secret;
  const bool retval = ReadKeys(key_file_name, &id, &secret);
  if (!retval)
    return GatewayKey();
  return GatewayKey(id, secret);
}

bool ReadKeys(const std::string &key_file_name, std::string *key_id,
              std::string *secret) {
  if (!(key_id && secret)) {
    return false;
  }

  const int key_file_fd = open(key_file_name.c_str(), O_RDONLY);
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

bool ParseKey(const std::string &body, std::string *key_id,
              std::string *secret) {
  const std::string line = GetLineMem(body.data(), body.size());
  std::string l = Trim(ReplaceAll(line, "\t", " "));
  l.erase(std::unique(l.begin(), l.end(), BothAreSpaces), l.end());
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
