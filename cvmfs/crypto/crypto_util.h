/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CRYPTO_CRYPTO_H_
#define CVMFS_CRYPTO_CRYPTO_H_

#include "util/export.h"

namespace crypto {

CVMFS_EXPORT void InitRng();

}  // namespace crypto

#endif  // CVMFS_CRYPTO_CRYPTO_H_
