/**
 * This file is part of the CernVM File System
 */

#ifndef CVMFS_WEBAPI_MACAROON_H_
#define CVMFS_WEBAPI_MACAROON_H_

#include <ctime>
#include <string>

#include "hash.h"
#include "json_document.h"
#include "util/single_copy.h"

namespace cipher {
class AbstractKeyDatabase;
class Key;
}


/**
 * An implementation of Macaroons: http://research.google.com/pubs/pub41892.html
 * Macaroons are tamper-proof authorization tokens with attenuations and
 * third-party caveats.
 *
 * Macaroons are issued by the octopus lease server in form of a lease.  The
 * lease server shares separate keys with each of the release manager machines
 * and with the storage relay.  Leases are targeted for the storage relay.  The
 * release manager machine is seen as a third party that signs off the hash of
 * the change packs.
 *
 * The following caveats are supported for the octopus server:
 *  - expiry time
 *  - fully qualified repository name
 *  - cvmfs sub tree
 *  - Required 3rd party (release manager magine) caveat: hash of change pack,
 *    request type (transaction start, new data packet, commit).
 *
 * Note that the in-memory representation does not fully mirror the JSON
 * representation.  The availability of the cryptographic keys depends on the
 * lifecycle of the macaroon.  As for the lifecyclt, a macaroon is
 *   -# created at the lease server ("ExportPrimary") that has access to both
 *      the shared secret key of the storage gateway and the shared secret key
 *      of the release manager machine;
 *   -# parsed on the release manager machine (publisher) that decrypts the
 *      "cld" field containing the random key used for 3rd-party caveats;
 *   -# attenuated on the release manager machin by 3rd-party caveats (begin
 *      transaction, change set hash, another change set hash, ..., publish
 *      transaction); multiple macaroons with different 3rd-party caveats are
 *      created here ("ExportDerived");
 *   -# parsed and verified on the storage relay using the private key from step
 *      1 and the "vld" field with the encrypted random key used for 3rd-party
 *      caveats.
 */
class Macaroon : SingleCopy {
 public:
  enum VerifyFailures {
    kFailOk = 0,
    kFailBadJson,
    kFailBadMacaroon,
    kFailBadPrimarySignature,
    kFailBad3rdPartySignature,
    kFailUnknownKey,
    kFailDecrypt,
    kFailExpired,
    kFailBadRootPath,
    kFailReplay,
    kFailUnknown,
  };

  enum PublishOperation {
    kPublishNoop = 0,
    kPublishTransaction,
    kPublishChangePack,
    kPublishCommit,
  };

  Macaroon(const std::string &key_id_storage,
           const cipher::Key *secret_key_storage,
           const std::string &key_id_publisher,
           const cipher::Key *secret_key_publisher);
  static Macaroon *ParseOnPublisher(const std::string &json,
                                    cipher::AbstractKeyDatabase *key_db,
                                    VerifyFailures *failure_code);
  static Macaroon *ParseOnStorage(const std::string &json,
                                  cipher::AbstractKeyDatabase *key_db,
                                  VerifyFailures *failure_code);
  ~Macaroon();

  void set_ttl(const unsigned ttl_s) {
    expiry_utc_ = time(NULL) + ttl_s;
  }
  void set_root_path(const std::string &root_path) { root_path_ = root_path; }
  void set_fqrn(const std::string &fqrn) { fqrn_ = fqrn; }
  void set_origin(const std::string &origin) { origin_ = origin; }
  void set_target(const std::string &target) { target_hint_ = target; }
  void set_publisher_hint(const std::string &hint) { publisher_hint_ = hint; }

  void set_payload_hash(const shash::Any &hash) { payload_hash_ = hash; }
  void set_publish_operation(PublishOperation op) { publish_operation_ = op; }
  void set_ttl_operation(const unsigned ttl_s) {
    expiry_utc_operation_ = time(NULL) + ttl_s;
  }

  std::string ExportPrimary();
  std::string ExportAttenuated();

 private:
  Macaroon();
  void ComputePrimaryHmac();
  void Compute3rdPartyHmac();
  std::string ExportPrimaryFields();
  std::string Export3rdPartyFields();
  bool CreateFromJson(JSON *json);
  bool Create3rdPartyFromJson(JSON *json);
  VerifyFailures ExtractOnetimeKey(const std::string &key_onetime4me,
                                   const cipher::Key &my_secret_key);

  /**
   * The unique identifier of the macaroon.  Serves also as transaction id: this
   * macaroon can only be used for a single transaction - change set - publish
   * cycle.
   */
  std::string random_nonce_;

  /**
   * The signing key used for the initial HMAC.  This is a key shared by the
   * lease service and the storage gateway (target service).
   */
  std::string key_id_storage_;

  /**
   * The key shared between the lease server and the release manager machine.
   * Used by the release manager machine to sign the 3rd party caveats.
   */
  std::string key_id_publisher_;

  /**
   * The key shared with the target service (storage relay).  Should match the
   * key_id_storage_.  NULL if created from JSON document.
   */
  const cipher::Key *secret_key_storage_;

  /**
   * The key shared with the release manager machine.  Should match the
   * key_id_publisher_.  NULL if created from JSON document.
   */
  const cipher::Key *secret_key_publisher_;

  /**
   * The machine that issued the macaroon
   */
  std::string origin_;

  /**
   * Location hint to the target service: storage relay or lease service.
   */
  std::string target_hint_;

  /**
   * If the macaroon is created from a JSON snippet, the hmac_ is stored here.
   * Otherwise the macaroon was created from scratch with the secret key and
   * hmac_ is empty.
   */
  shash::Any hmac_primary_;

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
   * Hint to the release manager machine.
   */
  std::string publisher_hint_;

  /**
   * The root key for the 3rd party caveats.  Random and unique to a macaroon.
   * The macaroon owns this key.
   */
  cipher::Key *key_onetime_;

  /**
   * The random key specific to this macaroon encrypted for the publisher.
   */
  std::string key_onetime4storage_;

  /**
   * The random key specific to this macaroon encrypted for the publisher.
   */
  std::string key_onetime4publisher_;

  /**
   * A message from the release manager machine to the storage relay about the
   * the operation that this macaroon discharges.
   */
  PublishOperation publish_operation_;

  /**
   * The time to live for the specific operation discharged by the macaroon.
   * This should be shorter than the macaroon's life time in expiry_utc_.
   */
  time_t expiry_utc_operation_;

  /**
   * Empty unless sent from release manager machine to the storage relay.  In
   * the latter case it is the hash of the change pack signed off by the release
   * manager machine.  Note that the release manager machine and the storage
   * relay do not share a key.  Only the lease service and the storage relay
   * share a key.
   */
  shash::Any payload_hash_;

  /**
   * The hmac protecting the caveats set by the release manager machine.
   */
  shash::Any hmac_3rd_party_;
};

#endif  // CVMFS_WEBAPI_MACAROON_H_
