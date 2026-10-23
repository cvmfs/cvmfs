/**
 * This file is part of the CernVM File System.
 */

#include "gateway_util.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <climits>
#include <vector>

#include "crypto/hash.h"
#include "util/posix.h"
#include "util/string.h"

namespace {

bool BothAreSpaces(const char &c1, const char &c2) {
  return c1 == ' ' && (c1 == c2);
}

}  // namespace

namespace gateway {

// The publisher advertises the highest gateway API protocol version it can
// speak. It must be at least kApiVersionNonexistentPath for the gateway to
// echo back a negotiated version that enables --allow-nonexistent-path.
int APIVersion() { return kApiVersionNonexistentPath; }

std::string SessionTokenApiVersionPath(const std::string &token_path) {
  return token_path + ".api_version";
}

std::string MakeSessionTokenApiVersionRecord(const int api_version,
                                             const std::string &token) {
  const shash::Md5 fingerprint(token.data(),
                               static_cast<unsigned>(token.size()));
  return StringifyInt(api_version) + "\n" + fingerprint.ToString(false);
}

bool ParseSessionTokenApiVersionRecord(const std::string &record,
                                       const std::string &token,
                                       int *api_version) {
  if (api_version == NULL)
    return false;

  const size_t separator = record.find('\n');
  uint64_t version;
  if (separator == std::string::npos
      || !String2Uint64Parse(record.substr(0, separator), &version)
      || version > static_cast<uint64_t>(INT_MAX)) {
    return false;
  }

  const shash::Md5 fingerprint(token.data(),
                               static_cast<unsigned>(token.size()));
  if (record.substr(separator + 1) != fingerprint.ToString(false)) {
    return false;
  }

  *api_version = static_cast<int>(version);
  return true;
}

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
