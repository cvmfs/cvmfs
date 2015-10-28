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
 * Macaroons are issued by the octopus lease server in form of a lease.  The 
 * lease server shares separate keys with each of the release manager machines
 * and with the storage relay.  Leases are targeted for the storage relay.  The 
 * release manager machine is seen as a third party that signs off the hash of 
 * the change pack.
 *
 * The following caveats are supported for the octopus server:
 *  - expiry time
 *  - fully qualified repository name
 *  - cvmfs sub tree
 *  - Required 3rd party (release manager magine) caveat: hash of change pack.
 *
 * Note that the in-memory representation does not fully mirror the JSON
 * representation.  The cryptographic bits and pieces (HMAC, encrypted keys) are
 * only created and parsed for the network transfer.
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
   * The signing key used for the initial HMAC.  This is a key shared by the
   * release manager machine and the lease service.
   */
  std::string key_id_;

  /**
   * The machine that issued the macaroon
   */
  std::string origin_;

  /**
   * Location hint to the target service: storage relay or lease service.
   */
  std::string target_hint_;


  // Caveats

  /**
   * Time to live for this macaroon
   */
  time_t expiry_utc_;

  /**
   * Repository name
   */
  std::string fqrn_;

  /**
   * Sub part of the name space
   */
  std::string root_path_;


  // 3rd-party caveat, slightly simplified: no target hint, no assertion

  /**
   * Empty unless sent from release manager machine to the storage relay.  In
   * the latter case it is the hash of the change pack signed of by the release
   * manager machine.  Note that the release manager machine and the storage
   * relay do not share a key.  Only the lease service and the storage relay
   * share a key.
   */
  std::string payload_hash_;
};

#endif  // CVMFS_WEBAPI_MACAROON_H_
