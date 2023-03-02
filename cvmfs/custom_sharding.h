// This file is part of the CernVM File System
//
#ifndef CVMFS_CUSTOM_SHARDING_H_
#define CVMFS_CUSTOM_SHARDING_H_ 1

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "util/logging.h"

class CustomSharding {
 public:
  CustomSharding();
  ~CustomSharding();
  void StartHealthCheck();
  void StopHealthCheck();
  void AddProxy(const std::string &proxy);
  const std::string &GetNextProxy(const std::string *url,
                           const std::string &current_proxy, off_t off);
};

#endif  // CVMFS_CUSTOM_SHARDING_H_
