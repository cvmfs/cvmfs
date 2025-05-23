/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NETWORK_HEALTH_CHECK_H_
#define CVMFS_NETWORK_HEALTH_CHECK_H_

#include "util/single_copy.h"

namespace download {

/**
 * Interface class for a continuous health check of the proxies used in the
 * download manager.
 *
 * Must be used by a specific implementation.
 * From the layout it is expected that the specific health check implementation
 * creates a thread/fork where the health check is continuously performed.
 *
 * @note compile-time interfaces are work in progress
 */
class HealthCheck : SingleCopy {
 public:
  HealthCheck() { }
  virtual ~HealthCheck() { }
  virtual void StartHealthcheck() = 0;
  virtual void StopHealthcheck() = 0;
};

}  // namespace download

#endif  // CVMFS_NETWORK_HEALTH_CHECK_H_
