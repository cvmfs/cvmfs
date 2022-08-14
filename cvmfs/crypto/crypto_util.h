/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CRYPTO_CRYPTO_UTIL_H_
#define CVMFS_CRYPTO_CRYPTO_UTIL_H_

#include "util/export.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

namespace crypto {

CVMFS_EXPORT void InitRng();

CVMFS_EXPORT void SetupLibcryptoMt();
CVMFS_EXPORT void CleanupLibcryptoMt();

}  // namespace crypto

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_CRYPTO_CRYPTO_UTIL_H_
