/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_EXCEPT_H_
#define CVMFS_PUBLISH_EXCEPT_H_

#include <stdexcept>
#include <string>

namespace publish {

class EPublish : public std::runtime_error {
 public:
  /**
   * Well-known exceptions that are usually caught and handled
   */
  enum EFailures {
    kFailUnspecified = 0,
    kFailInput,               // Invalid input
    kFailInvocation,          // Invalid command line options
    kFailPermission,          // Not owner of the repository
    kFailTransactionState,    // The txn was expected to be in the other state
    kFailGatewayKey,          // cannot access the gateway secret key
    kFailLeaseHttp,           // cannot connect to the gateway HTTP endpoint
    kFailLeaseBody,           // corrupted session token
    kFailLeaseBusy,           // another active lease blocks the path
    kFailLeaseNoEntry,        // the lease path does not exist
    kFailLeaseNoDir,          // the lease path is no a directory
    kFailRepositoryNotFound,  // the repository was not found on the machine
    kFailRepositoryType,      // the stratum type (0, 1) does not match
    kFailLayoutRevision,      // unsupported layout revision, migrate required
    kFailWhitelistExpired,    //
    kFailMissingDependency,   // a program or service was not found
  };

  explicit EPublish(const std::string& what, EFailures f = kFailUnspecified)
    : std::runtime_error(what + "\n\nStacktrace:\n" + GetStacktrace())
    , failure_(f)
    , msg_(what)
  {}

  virtual ~EPublish() throw();

  EFailures failure() const { return failure_; }
  std::string msg() const { return msg_; }

 private:
  EFailures failure_;
  std::string msg_;

  /**
   * Maximum number of frames in the stack trace
   */
  static const unsigned kMaxBacktrace = 64;
  static std::string GetStacktrace();
};

}  // namespace publish

#endif  // CVMFS_PUBLISH_EXCEPT_H_
