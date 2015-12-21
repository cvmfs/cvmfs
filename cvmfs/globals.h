/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_GLOBALS_H_
#define CVMFS_GLOBALS_H_

#include <unistd.h>

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

extern bool g_claim_ownership;
extern bool g_raw_symlinks;
extern uid_t g_uid;
extern gid_t g_gid;

#ifdef CVMFS_NAMESPACE_GUARD
}
#endif

#endif  // CVMFS_GLOBALS_H_
