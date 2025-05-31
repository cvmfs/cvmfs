/**
 * This file is part of the CernVM File System.
 *
 * This file contains a very simple implementation of session tokens.
 */

#include "session_token.h"

#include <limits>

#include "crypto/encrypt.h"
#include "json.h"
#include "json_document.h"
#include "util/logging.h"
#include "util/platform.h"
#include "util/pointer.h"
#include "util/string.h"

namespace receiver {

/**
 * Generate a session token from a public key_id, a path argument and a max
 * lease time (given in seconds).
 *
 * The session token encodes a lease valid for "path" until now() +
 * max_lease_time.
 *
 * Returns the session token, the (public) token_id and the token secret.
 */
bool GenerateSessionToken(const std::string &key_id, const std::string &path,
                          uint64_t max_lease_time, std::string *session_token,
                          std::string *public_token_id,
                          std::string *token_secret) {
  if (session_token == NULL || public_token_id == NULL
      || token_secret == NULL) {
    return false;
  }

  if (key_id.empty() && path.empty()) {
    return false;
  }

  const UniquePtr<cipher::Key> secret(cipher::Key::CreateRandomly(32));
  if (!secret.IsValid()) {
    return false;
  }

  const UniquePtr<cipher::Cipher> cipher(
      cipher::Cipher::Create(cipher::kAes256Cbc));
  if (!cipher.IsValid()) {
    return false;
  }

  *public_token_id = key_id + path;
  *token_secret = secret->ToBase64();

  const uint64_t current_time = platform_monotonic_time();
  if (std::numeric_limits<uint64_t>::max() - max_lease_time < current_time) {
    return false;
  }

  const std::string expiry(StringifyUint(current_time + max_lease_time));

  std::string encrypted_body;
  if (!cipher->Encrypt(
          "{\"path\" : \"" + path + "\", \"expiry\" : \"" + expiry + "\"}",
          *secret, &encrypted_body)) {
    return false;
  }

  *session_token = Base64("{\"token_id\" : \"" + *public_token_id
                          + "\", \"blob\" : \"" + Base64(encrypted_body)
                          + "\"}");

  return true;
}

/**
 * Obtain the public_id from a session token
 */

bool GetTokenPublicId(const std::string &token, std::string *public_id) {
  if (public_id == NULL) {
    return false;
  }

  std::string debased64_token;
  if (!Debase64(token, &debased64_token)) {
    return false;
  }

  const UniquePtr<JsonDocument> token_json(
      JsonDocument::Create(debased64_token));
  if (!token_json.IsValid()) {
    return false;
  }

  const JSON *token_id = JsonDocument::SearchInObject(token_json->root(),
                                                      "token_id", JSON_STRING);
  const JSON *blob = JsonDocument::SearchInObject(token_json->root(), "blob",
                                                  JSON_STRING);

  if (token_id == NULL || blob == NULL) {
    return false;
  }

  *public_id = token_id->string_value;

  return true;
}

/*
 * Check the validity of a session token using the associated secret
 */
TokenCheckResult CheckToken(const std::string &token, const std::string &secret,
                            std::string *lease_path) {
  if (!lease_path) {
    return kInvalid;
  }

  std::string debased64_token;
  if (!Debase64(token, &debased64_token)) {
    return kInvalid;
  }

  const UniquePtr<JsonDocument> token_json(
      JsonDocument::Create(debased64_token));
  if (!token_json.IsValid()) {
    return kInvalid;
  }

  const JSON *token_id = JsonDocument::SearchInObject(token_json->root(),
                                                      "token_id", JSON_STRING);
  const JSON *blob = JsonDocument::SearchInObject(token_json->root(), "blob",
                                                  JSON_STRING);
  if (token_id == NULL || blob == NULL) {
    return kInvalid;
  }

  std::string debased64_secret;
  if (!Debase64(secret, &debased64_secret)) {
    return kInvalid;
  }
  const UniquePtr<cipher::Key> key(
      cipher::Key::CreateFromString(debased64_secret));
  if (!key.IsValid()) {
    return kInvalid;
  }

  std::string encrypted_body;
  if (!Debase64(blob->string_value, &encrypted_body)) {
    return kInvalid;
  }

  std::string body;
  if (!cipher::Cipher::Decrypt(encrypted_body, *key, &body)) {
    return kInvalid;
  }

  const UniquePtr<JsonDocument> body_json(JsonDocument::Create(body));
  if (!token_json.IsValid()) {
    return kInvalid;
  }

  const JSON *path = JsonDocument::SearchInObject(body_json->root(), "path",
                                                  JSON_STRING);
  const JSON *expiry = JsonDocument::SearchInObject(body_json->root(), "expiry",
                                                    JSON_STRING);
  if (path == NULL || expiry == NULL) {
    return kInvalid;
  }

  // TODO(radu): can we still use monotonic time if the process restarts?
  const uint64_t expiry_time = String2Uint64(expiry->string_value);
  const uint64_t current_time = platform_monotonic_time();
  if (current_time > expiry_time) {
    return kExpired;
  }

  *lease_path = path->string_value;

  return kValid;
}

}  // namespace receiver
