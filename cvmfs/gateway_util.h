/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_GATEWAY_UTIL_H_
#define CVMFS_GATEWAY_UTIL_H_

#include <string>

namespace gateway {

struct GatewayKey {
 public:
  GatewayKey() { }
  GatewayKey(const std::string &i, const std::string &s)
      : id_(i), secret_(s) { }

  bool IsValid() const { return !id_.empty(); }
  std::string id() const { return id_; }
  std::string secret() const { return secret_; }

 private:
  std::string id_;
  std::string secret_;
};

int APIVersion();

GatewayKey ReadGatewayKey(const std::string &key_file_name);

bool ReadKeys(const std::string &key_file_name, std::string *key_id,
              std::string *secret);

bool ParseKey(const std::string &body, std::string *key_id,
              std::string *secret);

}  // namespace gateway

#endif  // CVMFS_GATEWAY_UTIL_H_
