/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_RECEIVER_SESSION_TOKEN_H_
#define CVMFS_RECEIVER_SESSION_TOKEN_H_

#include <stdint.h>

#include <string>

namespace receiver {

enum TokenCheckResult {
  kValid = 0,
  kInvalid,
  kExpired
};

bool GenerateSessionToken(const std::string &key_id, const std::string &path,
                          uint64_t max_lease_time, std::string *session_token,
                          std::string *public_token_id,
                          std::string *token_secret);

bool GetTokenPublicId(const std::string &token, std::string *public_id);

TokenCheckResult CheckToken(const std::string &token, const std::string &secret,
                            std::string *lease_path);

}  // namespace receiver

#endif  //  CVMFS_RECEIVER_SESSION_TOKEN_H_
