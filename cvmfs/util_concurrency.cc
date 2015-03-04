/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "util_concurrency.h"

#include <unistd.h>

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

unsigned int GetNumberOfCpuCores() {
  const int numCPU = sysconf(_SC_NPROCESSORS_ONLN);

  if (numCPU <= 0) {
    LogCvmfs(kLogSpooler, kLogWarning, "Unable to determine the available "
                                       "number of processors in the system... "
                                       "falling back to default '%d'",
             kFallbackNumberOfCpus);
    return kFallbackNumberOfCpus;
  }

  return static_cast<unsigned int>(numCPU);
}

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif
