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

CustomSharding::CustomSharding() {
  PANIC( kLogStderr, "Custom Sharding is unimplemented. Compile-time patching required"); //NOLINT
}

CustomSharding::~CustomSharding() {}

void CustomSharding::StartHealthCheck() {}
void CustomSharding::StopHealthCheck() {}
void CustomSharding::AddProxy(std::string proxy) {}
std::string CustomSharding::GetNextProxy(const std::string *url,
                                         const std::string current_proxy,
                                         off_t off) { return ""; }
