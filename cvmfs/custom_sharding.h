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
  void *(*sharding_init)(void);
  int (*sharding_free)(void *);
  int (*sharding_add_proxy)(void *, const char *);
  char *(*sharding_get_next_proxy)(void *, const char *, const char *,
                                   off_t off);
  void (*sharding_start_healthcheck)(void *);
  void (*sharding_stop_healthcheck)(void *);
  void (*sharding_register_log_callback)(void *, void (*)(const char *));

 public:
  CustomSharding();
  ~CustomSharding();
  void StartHealthCheck();
  void StopHealthCheck();
  void AddProxy(std::string proxy);
  std::string GetNextProxy(const std::string *url,
                           const std::string current_proxy, off_t off);
};

#ifdef __cplusplus
extern "C" {
#endif

void *shard_init(void);
int shard_free(void *data);
int shard_add_proxy(void *data, const char *proxy);
char *shard_get_next_proxy(void *data, const char *path,
                           const char *current_proxy, off_t offset);
void shard_healthcheck_start(void *data);
void shard_healthcheck_stop(void *data);

#ifdef __cplusplus
}
#endif

#endif  // CVMFS_CUSTOM_SHARDING_H_
