/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NETWORK_SHARDING_POLICY_H_
#define CVMFS_NETWORK_SHARDING_POLICY_H_

#include <string>

#include "util/single_copy.h"

namespace download {

// Return values of the sharding policy classes (including custom classes)
enum ShardingPolicyReturn {
  kShardingPolicySucess = 0,
  kShardingPolicyProxySwitch,
  kShardingPolicyProxyFail
};

// List of available custom ShardingPolicy classes
enum ShardingPolicySelector {
  kShardingPolicyExternal
};

/**
 * Interface class for a sharding policy that returns the optimal proxy
 * for each download request.
 */
class ShardingPolicy : SingleCopy {
 public:
  ShardingPolicy() { }
  virtual ~ShardingPolicy() { }

  virtual void AddProxy(const std::string &proxy) = 0;
  virtual std::string GetNextProxy(const std::string *url,
                                   const std::string &current_proxy,
                                   size_t off) = 0;
  // TODO(heretherebedragons) change return type to unsigned
  virtual int32_t GetNumberOfProxiesOnline() = 0;
  // TODO(heretherebedragons) change return type to std::vector<std::string>
  virtual std::string GetProxyList() = 0;
  virtual void LogProxyList() = 0;
};

}  // namespace download

#endif  // CVMFS_NETWORK_SHARDING_POLICY_H_
