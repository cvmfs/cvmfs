/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_DUPLEX_SSL_H_
#define CVMFS_DUPLEX_SSL_H_

#include <openssl/opensslv.h>

#if OPENSSL_VERSION_NUMBER >= 0x10100000L && !defined(LIBRESSL_VERSION_NUMBER)
#define OPENSSL_API_INTERFACE_V11
#elif OPENSSL_VERSION_NUMBER < 0x00908000L
#define OPENSSL_API_INTERFACE_V09
#else
#define OPENSSL_API_INTERFACE_V10
#endif

#endif  // CVMFS_DUPLEX_SSL_H_
