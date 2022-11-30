/**
 * This file is part of the CernVM File System.
 */

#include "globals.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

bool g_claim_ownership = false;
bool g_raw_symlinks = false;
uid_t g_uid = 0;
gid_t g_gid = 0;

#ifdef CVMFS_NAMESPACE_GUARD
}
#endif
