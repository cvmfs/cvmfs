/**
 * This file is part of the CernVM File System
 */

#include "cvmfs_config.h"
#include "macaroon.h"

#include <ctime>

#include "../encrypt.h"
#include "../hash.h"
#include "../sanitizer.h"
#include "../util/pointer.h"
#include "../util/string.h"
#include "../uuid.h"

using namespace std;  // NOLINT

Macaroon::Macaroon()
  : secret_key_storage_(NULL)
  , secret_key_publisher_(NULL)
  , expiry_utc_(0)
  , key_onetime_(NULL)
  , publish_operation_(kPublishNoop)
  , expiry_utc_operation_(0)
{
  hmac_primary_.algorithm = shash::kSha1;
  hmac_3rd_party_.algorithm = shash::kSha1;
  payload_hash_.algorithm = shash::kShake128;
}


/**
 * Used on the lease server where both keys are known.
 */
Macaroon::Macaroon(
  const string &key_id_storage,
  const cipher::Key *secret_key_storage,
  const string &key_id_publisher,
  const cipher::Key *secret_key_publisher)
  : random_nonce_(cvmfs::Uuid::CreateOneTime())
  , key_id_storage_(key_id_storage)
  , key_id_publisher_(key_id_publisher)
  , secret_key_storage_(secret_key_storage)
  , secret_key_publisher_(secret_key_publisher)
  , expiry_utc_(0)
  , key_onetime_(NULL)
  , publish_operation_(kPublishNoop)
  , expiry_utc_operation_(0)
{
  hmac_primary_.algorithm = shash::kSha1;

  UniquePtr<cipher::Cipher> cipher_aes(
    cipher::Cipher::Create(cipher::kAes256Cbc));
  assert(cipher_aes.IsValid());
  key_onetime_ = cipher::Key::CreateRandomly(cipher_aes->key_size());
  assert(key_onetime_ != NULL);

  string cipher_text;
  bool retval = cipher_aes->Encrypt(
    key_onetime_->ToBase64(), *secret_key_storage_, &cipher_text);
  assert(retval);
  key_onetime4storage_ = Base64(cipher_text);
  retval = cipher_aes->Encrypt(
    key_onetime_->ToBase64(), *secret_key_publisher_, &cipher_text);
  assert(retval);
  key_onetime4publisher_ = Base64(cipher_text);
}


Macaroon::~Macaroon() {
  delete key_onetime_;
}


/**
 * Creates the HMAC chain for the 3rd party caveats of the macaroon.  Uses the
 * one time key as a starting point of the HMAC chain.
 */
void Macaroon::Compute3rdPartyHmac() {
  assert(key_onetime_ != NULL);

  // Start with a base64 version of the secret key and the random nonce of the
  // primary macaroon, thereby binding the two together
  HmacString(key_onetime_->ToBase64(), random_nonce_, &hmac_3rd_party_);

  // Always the latest hmac_3rd_party_.ToString()
  string key_walker;

  key_walker = hmac_3rd_party_.ToString();
  int operation = publish_operation_;
  HmacString(key_walker, StringifyInt(operation), &hmac_3rd_party_);

  key_walker = hmac_3rd_party_.ToString();
  HmacString(key_walker, StringifyInt(expiry_utc_operation_), &hmac_3rd_party_);

  if (!payload_hash_.IsNull()) {
    key_walker = hmac_3rd_party_.ToString();
    HmacString(key_walker, payload_hash_.ToString(), &hmac_3rd_party_);
  }

  // Seal the HMAC
  key_walker = hmac_3rd_party_.ToString();
  HashString(key_walker, &hmac_3rd_party_);
}


/**
 * Creates the HMAC chain for the macaroon.  Computation from scratch can only
 * be done by the origin service and the taget service because only they possess
 * the secret key that starts the chain.
 */
void Macaroon::ComputePrimaryHmac() {
  assert(secret_key_storage_ != NULL);

  // Start with a base64 version of the secret key and the random nonce
  HmacString(secret_key_storage_->ToBase64(), random_nonce_, &hmac_primary_);

  // Always the latest hmac_.ToString()
  string key_walker;

  key_walker = hmac_primary_.ToString();
  HmacString(key_walker, key_id_storage_, &hmac_primary_);

  key_walker = hmac_primary_.ToString();
  HmacString(key_walker, key_id_publisher_, &hmac_primary_);

  key_walker = hmac_primary_.ToString();
  HmacString(key_walker, origin_, &hmac_primary_);

  key_walker = hmac_primary_.ToString();
  HmacString(key_walker, target_hint_, &hmac_primary_);

  key_walker = hmac_primary_.ToString();
  HmacString(key_walker, StringifyInt(expiry_utc_), &hmac_primary_);

  key_walker = hmac_primary_.ToString();
  HmacString(key_walker, fqrn_, &hmac_primary_);

  key_walker = hmac_primary_.ToString();
  HmacString(key_walker, root_path_, &hmac_primary_);

  key_walker = hmac_primary_.ToString();
  HmacString(key_walker, publisher_hint_, &hmac_primary_);

  key_walker = hmac_primary_.ToString();
  HmacString(key_walker, key_onetime4storage_, &hmac_primary_);

  key_walker = hmac_primary_.ToString();
  HmacString(key_walker, key_onetime4publisher_, &hmac_primary_);

  // Seal the HMAC
  key_walker = hmac_primary_.ToString();
  HashString(key_walker, &hmac_primary_);
}


bool Macaroon::Create3rdPartyFromJson(JSON *json) {
  JSON *walker = json;
  while (walker != NULL) {
    if (walker->name == NULL)
      return false;
    string name = walker->name;
    json_type type = walker->type;

    // Known field types
    if (name == "publish_operation") {
      if (type != JSON_INT) return false;
      if (walker->int_value < kPublishTransaction) return false;
      if (walker->int_value > kPublishCommit) return false;
      publish_operation_ = PublishOperation(walker->int_value);
    } else if (name == "expiry_utc_operation") {
      if (type != JSON_INT) return false;
      if (walker->int_value < 0) return false;
      expiry_utc_operation_ = walker->int_value;
    } else if (name == "payload_hash") {
      if (type != JSON_STRING) return false;
      if (walker->string_value == NULL) return false;
      payload_hash_ =
        shash::MkFromHexPtr(shash::HexPtr(string(walker->string_value)));
    } else if (name == "hmac") {
      if (type != JSON_STRING) return false;
      if (walker->string_value == NULL) return false;
      hmac_3rd_party_ =
        shash::MkFromHexPtr(shash::HexPtr(string(walker->string_value)));
    }

    walker = walker->next_sibling;
  }
  return true;
}


bool Macaroon::CreateFromJson(JSON *json) {
  // Search "cvmfs_macaroon_v1 sub structure"
  JSON *walker = json;
  while (walker) {
    if ((string(walker->name) == "cvmfs_macaroon_v1") &&
        (walker->type == JSON_OBJECT))
    {
      walker = walker->first_child;
      break;
    }
    walker = walker->next_sibling;
  }
  if (walker == NULL)
    return false;

  // We are now in the cvmfs_macaroon_v1 sub structure
  sanitizer::UuidSanitizer uuid_sanitizer;
  sanitizer::UriSanitizer uri_sanitizer;
  sanitizer::RepositorySanitizer repo_sanitizer;
  sanitizer::Base64Sanitizer base64_sanitizer;
  do {
    if (walker->name == NULL)
      return false;
    string name = walker->name;
    json_type type = walker->type;

    // Known field types
    if (name == "random_nonce") {
      if (type != JSON_STRING) return false;
      if (walker->string_value == NULL) return false;
      if (!uuid_sanitizer.IsValid(walker->string_value)) return false;
      random_nonce_ = walker->string_value;
    } else if (name == "key_id_storage") {
      if (type != JSON_STRING) return false;
      if (walker->string_value == NULL) return false;
      if (!uri_sanitizer.IsValid(walker->string_value)) return false;
      key_id_storage_ = walker->string_value;
    } else if (name == "key_id_storage") {
      if (type != JSON_STRING) return false;
      if (walker->string_value == NULL) return false;
      if (!uri_sanitizer.IsValid(walker->string_value)) return false;
      key_id_storage_ = walker->string_value;
    } else if (name == "key_id_publisher") {
      if (type != JSON_STRING) return false;
      if (walker->string_value == NULL) return false;
      if (!uri_sanitizer.IsValid(walker->string_value)) return false;
      key_id_publisher_ = walker->string_value;
    } else if (name == "origin") {
      if (type != JSON_STRING) return false;
      if (walker->string_value == NULL) return false;
      if (!uri_sanitizer.IsValid(walker->string_value)) return false;
      origin_ = walker->string_value;
    } else if (name == "target_hint") {
      if (type != JSON_STRING) return false;
      if (walker->string_value == NULL) return false;
      if (!uri_sanitizer.IsValid(walker->string_value)) return false;
      target_hint_ = walker->string_value;
    } else if (name == "expiry_utc") {
      if (type != JSON_INT) return false;
      if (walker->int_value < 0) return false;
      expiry_utc_ = walker->int_value;
    } else if (name == "fqrn") {
      if (type != JSON_STRING) return false;
      if (walker->string_value == NULL) return false;
      if (!repo_sanitizer.IsValid(walker->string_value)) return false;
      fqrn_ = walker->string_value;
    } else if (name == "root_path") {
      if (type != JSON_STRING) return false;
      if (walker->name == NULL) return false;
      root_path_ = walker->string_value;
    } else if (name == "publisher_hint") {
      if (type != JSON_STRING) return false;
      if (walker->string_value == NULL) return false;
      if (!uri_sanitizer.IsValid(walker->string_value)) return false;
      publisher_hint_ = walker->string_value;
    } else if (name == "vid") {
      if (type != JSON_STRING) return false;
      if (walker->string_value == NULL) return false;
      if (!base64_sanitizer.IsValid(walker->string_value)) return false;
      key_onetime4storage_ = walker->string_value;
    } else if (name == "cid") {
      if (type != JSON_STRING) return false;
      if (walker->string_value == NULL) return false;
      if (!base64_sanitizer.IsValid(walker->string_value)) return false;
      key_onetime4publisher_ = walker->string_value;
    } else if (name == "hmac") {
      if (type != JSON_STRING) return false;
      if (walker->string_value == NULL) return false;
      hmac_primary_ =
        shash::MkFromHexPtr(shash::HexPtr(string(walker->string_value)));
    } else if ((name == "publisher_caveats") && (type == JSON_OBJECT)) {
      walker = walker->first_child;
      if (!Create3rdPartyFromJson(walker)) return false;
      walker = walker->parent;
    }

    walker = walker->next_sibling;
  } while (walker);

  return true;
}


/**
 * Used on the release manager machine.  The primary macaroon is taken as is,
 * only the 3rd party caveats are added.
 */
std::string Macaroon::ExportAttenuated() {
  Compute3rdPartyHmac();
  string json_text = string("{\"cvmfs_macaroon_v1\":{") +
    ExportPrimaryFields() +
    ",\"hmac\":\"" + hmac_primary_.ToString() + "\"" +
    "\"publisher_caveats\":{" +
    Export3rdPartyFields() +
    ",\"hmac\":\"" + hmac_3rd_party_.ToString() + "\"" +
    "}}}";

  UniquePtr<JsonDocument> json_document(JsonDocument::Create(json_text));
  assert(json_document.IsValid());
  return json_document->PrintCanonical();
}


/**
 * The order of the JSON fields matters because the HMAC is constructed
 * field-by-field.  Requires knowledge of the secret storage key.  Used by the
 * lease server.
 */
string Macaroon::ExportPrimary() {
  ComputePrimaryHmac();
  string json_text = string("{\"cvmfs_macaroon_v1\":{") +
    ExportPrimaryFields() +
    ",\"hmac\":\"" + hmac_primary_.ToString() + "\"" +
    "}}";
  UniquePtr<JsonDocument> json_document(JsonDocument::Create(json_text));
  assert(json_document.IsValid());
  return json_document->PrintCanonical();
}


/**
 * Export the inner guts of the 3rd party caveats without hmac.
 */
std::string Macaroon::Export3rdPartyFields() {
  return "\"publish_operation\":" + StringifyInt(publish_operation_) +
    ",\"expiry_utc_operation\":" + StringifyInt(expiry_utc_operation_) +
    (payload_hash_.IsNull()
      ? ""
      : ("\"payload_hash\":\"" + payload_hash_.ToString() + "\""));
}



/**
 * Export the inner guts of the primary macaroon without hmac.
 */
std::string Macaroon::ExportPrimaryFields() {
  return "\"random_nonce\":\"" + random_nonce_ + "\"" +
    "\"key_id_storage\":\"" + key_id_storage_ + "\"" +
    "\"key_id_publisher\":\"" + key_id_publisher_ + "\"" +
    ",\"origin\":\"" + origin_ + "\"" +
    ",\"target_hint\":\"" + target_hint_ + "\"" +
    ",\"expiry_utc\":" + StringifyInt(expiry_utc_) +
    ",\"fqrn\":\"" + fqrn_ + "\"" +
    ",\"root_path\":\"" + root_path_ + "\"" +
    ",\"publisher_hint\":\"" + publisher_hint_ + "\"" +
    ",\"vid\":\"" + key_onetime4storage_ + "\"" +
    ",\"cid\":\"" + key_onetime4publisher_ + "\"";
}


/**
 * The secret_key is either the publisher's one or the storage's one, depending
 * on where the method is called.
 */
Macaroon::VerifyFailures Macaroon::ExtractOnetimeKey(
  const string &key_onetime4me,
  const cipher::Key &my_secret_key)
{
  string ciphertext;
  string plaintext;
  string key_raw;
  bool retval = Debase64(key_onetime4me, &ciphertext);
  if (!retval) {
    return kFailBadMacaroon;
  }
  retval = cipher::Cipher::Decrypt(
    ciphertext, my_secret_key, &plaintext);
  if (!retval || (plaintext == "")) {
    return kFailDecrypt;
  }
  retval = Debase64(plaintext, &key_raw);
  if (!retval || (key_raw.size() != cipher::CipherAes256Cbc::kKeySize)) {
    return kFailDecrypt;
  }
  key_onetime_ = cipher::Key::CreateFromString(key_raw);
  assert(key_onetime_);
  return kFailOk;
}


/**
 * Creates a macaroon based on the JSON object that the lease server produced.
 */
Macaroon *Macaroon::ParseOnPublisher(
  const string &json,
  cipher::AbstractKeyDatabase *key_db,
  VerifyFailures *failure_code)
{
  assert(key_db != NULL);
  assert(failure_code != NULL);
  *failure_code = kFailUnknown;

  UniquePtr<JsonDocument> json_document(JsonDocument::Create(json));
  if (!json_document.IsValid()) {
    *failure_code = kFailBadJson;
    return NULL;
  }

  UniquePtr<Macaroon> macaroon(new Macaroon());
  bool retval = macaroon->CreateFromJson(json_document->root()->first_child);
  if (!retval) {
    *failure_code = kFailBadMacaroon;
    return NULL;
  }
  if (macaroon->expiry_utc_ < time(NULL)) {
    *failure_code = kFailExpired;
    return NULL;
  }

  // Extract the random root key for 3rd party caveats
  macaroon->secret_key_publisher_ = key_db->Find(macaroon->key_id_publisher_);
  if (macaroon->secret_key_publisher_ == NULL) {
    *failure_code = kFailUnknownKey;
    return NULL;
  }
  *failure_code = macaroon->ExtractOnetimeKey(macaroon->key_onetime4publisher_,
                                              *macaroon->secret_key_publisher_);
  if (*failure_code != kFailOk)
    return NULL;
  return macaroon.Release();
}


/**
 * Creates a macaroon based on the JSON object that the lease server produced
 * and the release manager machine attenuated.
 */
Macaroon *Macaroon::ParseOnStorage(
  const string &json,
  cipher::AbstractKeyDatabase *key_db,
  VerifyFailures *failure_code)
{
  assert(key_db != NULL);
  assert(failure_code != NULL);
  *failure_code = kFailUnknown;

  UniquePtr<JsonDocument> json_document(JsonDocument::Create(json));
  if (!json_document.IsValid()) {
    *failure_code = kFailBadJson;
    return NULL;
  }

  UniquePtr<Macaroon> macaroon(new Macaroon());
  bool retval =
    macaroon->CreateFromJson(json_document->root()->first_child);
  if (!retval) {
    *failure_code = kFailBadMacaroon;
    return NULL;
  }
  time_t now = time(NULL);
  if ((macaroon->expiry_utc_ < now) || (macaroon->expiry_utc_operation_ < now))
  {
    *failure_code = kFailExpired;
    return NULL;
  }

  // Extract the random root key for 3rd party caveats
  macaroon->secret_key_storage_ = key_db->Find(macaroon->key_id_storage_);
  if (macaroon->secret_key_storage_ == NULL) {
    *failure_code = kFailUnknownKey;
    return NULL;
  }
  *failure_code = macaroon->ExtractOnetimeKey(macaroon->key_onetime4storage_,
                                              *macaroon->secret_key_storage_);
  if (*failure_code != kFailOk)
    return NULL;

  shash::Any retrieved_primary_hmac(macaroon->hmac_primary_);
  macaroon->ComputePrimaryHmac();
  if (retrieved_primary_hmac != macaroon->hmac_primary_) {
    *failure_code = kFailBadPrimarySignature;
    return NULL;
  }

  shash::Any retrieved_3rdparty_hmac(macaroon->hmac_3rd_party_);
  macaroon->Compute3rdPartyHmac();
  if (retrieved_3rdparty_hmac != macaroon->hmac_3rd_party_) {
    *failure_code = kFailBad3rdPartySignature;
    return NULL;
  }

  return macaroon.Release();
}
