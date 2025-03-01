/**
 * This file is part of the CernVM File System.
 *
 * Shared data structures for client-side authorization.
 */

#ifndef CVMFS_AUTHZ_AUTHZ_H_
#define CVMFS_AUTHZ_AUTHZ_H_

#include <inttypes.h>

#include <string>

/**
 * X.509 certificates are needed by the download manager to authenticate the
 * user.  Other credential types might be added in the future.
 */
enum AuthzTokenType {
  kTokenUnknown = 0,
  kTokenX509,
  kTokenBearer,
};

/**
 * The credentials as a bag of bytes that can be passed to the download
 * manager.  Ownership of tokens is with the AuthzSessionManager.
 */
struct AuthzToken {
  AuthzToken() : type(kTokenUnknown), data(NULL), size(0) { }
  AuthzToken *DeepCopy();

  AuthzTokenType type;
  void *data;
  unsigned size;
};


enum AuthzStatus {
  kAuthzOk = 0,
  kAuthzNotFound,
  kAuthzInvalid,
  kAuthzNotMember,
  kAuthzNoHelper,
  kAuthzUnknown,
};


/**
 * The credentials together with the membership string it was verified for.
 * Entries expire.  Negative credential verification can be represented, too,
 * with status != kAuthzOk.
 */
struct AuthzData {
  AuthzData() : deadline(0), status(kAuthzUnknown) { }
  /**
   * The verification of the deadline is not included.
   */
  bool IsGranted(const std::string &expected_membership) const {
    return (status == kAuthzOk) && (membership == expected_membership);
  }
  AuthzToken token;
  uint64_t deadline;
  std::string membership;
  AuthzStatus status;
};

#endif  // CVMFS_AUTHZ_AUTHZ_H_
