/**
 * This file is part of the CernVM File System
 */

#ifndef CVMFS_WEBAPI_MACAROON_H_
#define CVMFS_WEBAPI_MACAROON_H_

#include <ctime>
#include <string>

/**
 * An implementation of Macaroons: http://research.google.com/pubs/pub41892.html
 * Macaroons are tamper-proof authorization tokens with attenuations and
 * third-party caveats.
 *
 * Macaroons are issued by the octopus lease server.  The lease server shares
 * separate keys with each of the release manager machines and with the storage
 * relay.  Leases are targeted for the storage array.  The release manager
 * machine is seen as a third party that signes off the hash of the change pack.
 *
 * The following caveats are supported for the octopus server:
 *  - expiry time
 *  - fully qualified repository name
 *  - cvmfs sub tree
 *  - [ perhaps origin at some point ]
 *  - Required 3rd party (release manager magine) caveat: hash of change pack.
 */
class Macaroon {
 public:
  enum VerifyFailures {
    kFailOk = 0,
    kFailBadJson,
    kFailNoSignature,
    kFailBadSignature,
    kFailExpired,
    kFailBadRootPath,
    kFailReplay,
  };

  Macaroon();
  void set_timeout(const unsigned timeout_s) {
    expiry_utc_ = time(NULL) + timeout_s;
  }
  void set_root_path(const std::string &root_path) { root_path_ = root_path; }
  void set_fqrn(const std::string &fqrn) { fqrn_ = fqrn; }
  void set_target(const std::string &target) { target_hint_ = target; }

  void ToJson();

 private:
  std::string ComputeHmac();

  /**
   * Unique ID of the macaroon.
   */
  std::string nonce_;

  /**
   * Location hint to the storage relay.
   */
  std::string target_hint_;

  // Caveats
  time_t expiry_utc_;
  std::string fqrn_;
  std::string root_path_;
  // 3rd-party caveat
};

#endif  // CVMFS_WEBAPI_MACAROON_H_
