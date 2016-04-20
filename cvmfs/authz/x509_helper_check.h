/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_AUTHZ_X509_HELPER_CHECK_H_
#define CVMFS_AUTHZ_X509_HELPER_CHECK_H_

#include <string>

bool CheckX509Proxy(const std::string &x509_proxy, 
                    const std::string &membership);

#endif  // CVMFS_AUTHZ_X509_HELPER_CHECK_H_
