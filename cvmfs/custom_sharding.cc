// This file is part of the CernVM File System
//
#include "custom_sharding.h"

#include <assert.h>
#include <dlfcn.h>

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "util/logging.h"

using namespace std;  // NOLINT

extern "C" {
static void __log(const char *str) {
  LogCvmfs(kLogDownload, kLogDebug, "CustomSharding log: [%s]", str);
}
}

CustomSharding::CustomSharding() {
  dso_object = dlopen("libcvmfs_custom_sharding.so", RTLD_NOW);

  if (!dso_object) {
    LogCvmfs(kLogCvmfs, kLogDebug, "CustomSharding dlopen failed: %s",
             dlerror());
    assert(dso_object != NULL);
  } else {
    sharding_init =
        reinterpret_cast<void *(*)(void)>(dlsym(dso_object, "shard_init"));
    sharding_free =
        reinterpret_cast<int (*)(void *)>(dlsym(dso_object, "shard_free"));
    sharding_add_proxy = reinterpret_cast<int (*)(void *, const char *)>(
        dlsym(dso_object, "shard_add_proxy"));
    sharding_get_next_proxy =
        reinterpret_cast<char *(*)(void *, const char *, const char *, off_t)>(
            dlsym(dso_object, "shard_get_next_proxy"));
    sharding_start_healthcheck = reinterpret_cast<void (*)(void *)>(
        dlsym(dso_object, "shard_healthcheck_start"));
    sharding_stop_healthcheck = reinterpret_cast<void (*)(void *)>(
        dlsym(dso_object, "shard_healthcheck_stop"));
    sharding_register_log_callback =
        reinterpret_cast<void (*)(void *, void (*)(const char *))>(
            dlsym(dso_object, "shard_register_log_callback"));
  }
  if (!sharding_init || !sharding_free || !sharding_add_proxy ||
      !sharding_get_next_proxy || !sharding_start_healthcheck ||
      !sharding_stop_healthcheck || !sharding_register_log_callback) {
    LogCvmfs(kLogCvmfs, kLogDebug, "One or more dlsym failed: %s", dlerror());
    dlclose(dso_object);
    dso_object = NULL;
    assert(dso_object != NULL);
  }
  dso_data = sharding_init();
  sharding_register_log_callback(dso_data, __log);
}

CustomSharding::~CustomSharding() {
  sharding_free(dso_data);
  dlclose(dso_object);
}

void CustomSharding::StartHealthCheck() {
  sharding_start_healthcheck(dso_data);
}
void CustomSharding::StopHealthCheck() { sharding_stop_healthcheck(dso_data); }
void CustomSharding::AddProxy(std::string proxy) {
  sharding_add_proxy(dso_data, proxy.c_str());
}
std::string CustomSharding::GetNextProxy(const std::string *url,
                                         const std::string current_proxy,
                                         off_t off) {
  std::string ret = current_proxy;
  const char *curp = NULL;
  const char *curlu = NULL;

  if (current_proxy != "") {
    curp = current_proxy.c_str();
  }
  if (off < 0) {
    off = 0;
  }
  if (url == NULL) {
    curlu = "";
  } else {
    curlu = url->c_str();
  }

  char *tmp = sharding_get_next_proxy(dso_data, curlu, curp, off);
  assert(tmp != NULL);
  ret = std::string(tmp);
  free(tmp);

  LogCvmfs(kLogCvmfs, kLogDebug,
           "CustomSharding::GetNextProxy: url [%s]  current_proxy [%s] ofset "
           "[%ld] next_proxy [%s]",
           url ? url->c_str() : "NONE",
           current_proxy != "" ? current_proxy.c_str() : "NONE", off,
           ret.c_str());

  return ret;
}
