/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_AUTHZ_X509_HELPER_FETCH_H_
#define CVMFS_AUTHZ_X509_HELPER_FETCH_H_

#include <unistd.h>

#include <string>

#include "authz/x509_helper_req.h"

bool GetX509Proxy(const AuthzRequest &authz_req, std::string *proxy);

#endif  // CVMFS_AUTHZ_X509_HELPER_FETCH_H_
