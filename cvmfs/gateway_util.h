/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_GATEWAY_UTIL_H_
#define CVMFS_GATEWAY_UTIL_H_

#include <string>

namespace gateway {

int APIVersion();

bool ReadKeys(const std::string& key_file_name, std::string* key_id,
              std::string* secret);

}  // namespace gateway

#endif  // CVMFS_GATEWAY_UTIL_H_
