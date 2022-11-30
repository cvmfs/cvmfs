// This file is part of the CernVM File System
//
#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "custom_sharding.h"
#include "util/logging.h"

using namespace std; // NOLINT

CustomSharding:: CustomSharding() {
    LogCvmfs(kLogCvmfs, kLogDebug, "CustomSharding constructor run");
}
CustomSharding:: ~CustomSharding() {
    LogCvmfs(kLogCvmfs, kLogDebug, "CustomSharding destructor run");
}

void CustomSharding::StartHealthCheck() {
    LogCvmfs(kLogCvmfs, kLogDebug, "CustomSharding healthcheck start");
}
void CustomSharding::StopHealthCheck() {
    LogCvmfs(kLogCvmfs, kLogDebug, "CustomSharding healthcheck stop");
}
void CustomSharding::AddProxy(std::string proxy) {
    LogCvmfs(kLogCvmfs, kLogDebug, "CustomSharding add proxy [%s]",
             proxy.c_str());
    proxies.push_back(proxy);
}
std::string CustomSharding::GetNextProxy(std::string url,
            std::string current_proxy, size_t off) {
    std::string ret = proxies[ rand() % proxies.size() ];
    LogCvmfs(kLogCvmfs, kLogDebug, "CustomSharding current proxy is [%s], returning randomized proxy [%s]", current_proxy.c_str(), ret.c_str()); // NOLINT
    return ret;
}


