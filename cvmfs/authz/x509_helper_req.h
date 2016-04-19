/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_AUTHZ_X509_HELPER_REQ_H_
#define CVMFS_AUTHZ_X509_HELPER_REQ_H_

#include <unistd.h>

#include <string>

const unsigned kProtocolVersion = 1;

struct AuthzRequest {
  AuthzRequest() : uid(-1), gid(-1), pid(-1) { }
  uid_t uid;
  gid_t gid;
  pid_t pid;
  std::string membership;

  std::string Ident() const;
};

#endif  // CVMFS_AUTHZ_X509_HELPER_REQ_H_
