/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CRYPTO_OPENSSL_VERSION_H_
#define CVMFS_CRYPTO_OPENSSL_VERSION_H_

#include <openssl/opensslv.h>

// Safeguard when compiling libcvmfs_crypto: make sure we pick up the built-in
// LibreSSL and not the system's OpenSSL
#ifdef CVMFS_LIBRARY
#ifndef LIBRESSL_VERSION_NUMBER
#error "picking up OpenSSL includes instead of LibreSSL"
#endif
#endif

#if OPENSSL_VERSION_NUMBER >= 0x10100000L
#define OPENSSL_API_INTERFACE_V11
#if OPENSSL_VERSION_NUMBER >= 0x10101000L
#define OPENSSL_API_INTERFACE_V111
#endif
#elif OPENSSL_VERSION_NUMBER < 0x00908000L
#define OPENSSL_API_INTERFACE_V09
#else
#define OPENSSL_API_INTERFACE_V10
#endif

#endif  // CVMFS_CRYPTO_OPENSSL_VERSION_H_
