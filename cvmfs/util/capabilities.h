/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CAPABILITIES_H_
#define CVMFS_CAPABILITIES_H_

#include <vector>

#ifdef __APPLE__
typedef int cap_value_t;
#define CAP_DAC_READ_SEARCH 0
#define CAP_SYS_ADMIN 0
#define CAP_SYS_PTRACE 0
#define CAP_SETUID 0
#define CAP_SETGID 0
#define CAP_SETPCAP 0
#else
#include <sys/capability.h>
#endif

#include "util/export.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

CVMFS_EXPORT bool ObtainDacReadSearchCapability();
CVMFS_EXPORT bool DropDacReadSearchCapability();
CVMFS_EXPORT bool ObtainSysAdminCapability();
CVMFS_EXPORT bool ObtainSysPtraceCapability();
CVMFS_EXPORT bool DropSysPtraceCapability();
CVMFS_EXPORT bool ObtainSetuidgidCapabilities(const bool avoid_mutexes = false);
CVMFS_EXPORT bool ObtainSetpcapCapability();
CVMFS_EXPORT bool SetuidCapabilityPermitted();
CVMFS_EXPORT bool SetpcapCapabilityPermitted();
CVMFS_EXPORT bool ClearPermittedCapabilities(
                   const std::vector<cap_value_t> &reservecaps,
                   const std::vector<cap_value_t> &inheritcaps);

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_CAPABILITIES_H_
