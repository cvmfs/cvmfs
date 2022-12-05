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
 private:
  std::vector<std::string> proxies;
  void *dso_data;

  void *dso_object;
  void* (*sharding_init)( void );
  int   (*sharding_free)( void* );
  int   (*sharding_add_proxy)( void *, const char * );
  char* (*sharding_next_proxy)( void *, const char*, const char*, size_t off );
  void  (*sharding_start_healthcheck)( void * );
  void  (*sharding_stop_healthcheck)( void * );
 public:
  CustomSharding();
  ~CustomSharding();
  void StartHealthCheck();
  void StopHealthCheck();
  void AddProxy(std::string proxy);
  std::string GetNextProxy(std::string url,
              std::string current_proxy, size_t off);
};

#endif  // CVMFS_CUSTOM_SHARDING_H_
