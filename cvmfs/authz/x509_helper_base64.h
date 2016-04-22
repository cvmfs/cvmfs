/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_AUTHZ_X509_HELPER_BASE64_H_
#define CVMFS_AUTHZ_X509_HELPER_BASE64_H_

#include <string>

std::string Base64(const std::string &data);
std::string Debase64(const std::string &data);

#endif  // CVMFS_AUTHZ_X509_HELPER_BASE64_H_
