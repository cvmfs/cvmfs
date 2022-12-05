// This file is part of the CernVM File System
//
#include "custom_sharding.h"
#include <dlfcn.h>

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "util/logging.h"

using namespace std; // NOLINT

CustomSharding:: CustomSharding() {
    dso_object = dlopen("libcvmfs_custom_sharding.so", RTLD_NOW);
    if (!dso_object) {
      LogCvmfs(kLogCvmfs, kLogDebug, "dlopen failed: %s", dlerror());
      return;
    } else {
      sharding_init = (void * (*) (void) )dlsym(dso_object, "shard_init");
      sharding_free = (int (*)(void *)) dlsym(dso_object, "shard_free");
      sharding_add_proxy = (int (*)(void *, const char*))
           dlsym(dso_object, "shard_add_proxy");
      sharding_get_next_proxy =
           (char* (*)(void *, const char *, const char *, size_t))
           dlsym(dso_object, "shard_get_next_proxy");
      sharding_start_healthcheck = (void (*)(void*))
           dlsym(dso_object, "shard_start_healthcheck");
      sharding_stop_healthcheck = (void (*)(void*))
           dlsym(dso_object, "shard_stop_healthcheck");
    }
    if ( !sharding_init || !sharding_free
        || !sharding_add_proxy
        || !sharding_get_next_proxy
        || !sharding_start_healthcheck || !sharding_stop_healthcheck ) {
      LogCvmfs(kLogCvmfs, kLogDebug,
           "One or more dlsym failed: %s", dlerror() );
      dlclose(dso_object);
      dso_object = NULL;
      return;
    }
    dso_data = sharding_init();
}

CustomSharding:: ~CustomSharding() {
    if (dso_object) { sharding_free( dso_data ); }
    if (dso_object) { dlclose( dso_object ); }
}

void CustomSharding::StartHealthCheck() {
    if (dso_object) { sharding_start_healthcheck( dso_data ); }
}
void CustomSharding::StopHealthCheck() {
    if (dso_object) { sharding_stop_healthcheck( dso_data ); }
}
void CustomSharding::AddProxy(std::string proxy) {
    if (dso_object) { sharding_add_proxy( dso_data, proxy.c_str() ); }
}
std::string CustomSharding::GetNextProxy(std::string url,
            std::string current_proxy, size_t off) {
    if (dso_object) { return std::string(sharding_get_next_proxy(dso_data,
                       url.c_str(), current_proxy.c_str(), off)); }

    return "";
}


