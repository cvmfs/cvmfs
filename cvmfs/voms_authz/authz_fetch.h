/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_VOMS_AUTHZ_AUTHZ_FETCH_H_
#define CVMFS_VOMS_AUTHZ_AUTHZ_FETCH_H_

#include <string>

#include "voms_authz/authz.h"

class AuthzFetcher {
 public:
  /**
   * Based on the current client context (pid, uid, gid) and the given
   * membership requirement, gather credentials.  Positive and negative replies
   * have a time to live.
   */
  virtual AuthzStatus FetchWithinClientCtx(const std::string &membership,
                                           AuthzToken *authz_token,
                                           unsigned *ttl) = 0;
};


/**
 * Defines the result on construction.  Used in libcvmfs and for testing.
 */
class AuthzStaticFetcher : public AuthzFetcher {
 public:
  AuthzStaticFetcher(AuthzStatus s, unsigned ttl) : status_(s), ttl_(ttl) { }
  virtual AuthzStatus FetchWithinClientCtx(const std::string &membership,
                                           AuthzToken *authz_token,
                                           unsigned *ttl)
  {
    *authz_token = AuthzToken();
    *ttl = ttl_;
    return status_;
  }

 private:
  AuthzStatus status_;
  unsigned ttl_;
};

#endif  // CVMFS_VOMS_AUTHZ_AUTHZ_FETCH_H_
