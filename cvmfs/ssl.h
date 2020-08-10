/**
 * This file is part of the CernVM File System.
 *
 * It helps managing the CA certificates for the servers mostly to enable
 * HTTPS connections.
 *
 */

#ifndef CVMFS_SSL_H_
#define CVMFS_SSL_H_

#include <string>
#include <vector>

#include "duplex_curl.h"
#include "util/posix.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

bool HasCertificates(std::string directory);

bool AddSSLCertificates(CURL *handle);

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_SSL_H_
