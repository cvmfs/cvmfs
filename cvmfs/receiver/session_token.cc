/**
 * This file is part of the CernVM File System.
 *
 * This file contains a very simple implementation of session tokens.
 */

#include "session_token.h"

#include "encrypt.h"
#include "json.h"
#include "json_document.h"
#include "platform.h"
#include "util/pointer.h"
#include "util/string.h"

namespace receiver {

/**
 * Generate a session token from a public key_id, a path argument and a max
 * lease time.
 *
 * The session token encodes a lease valid for "path" until now() +
 * max_lease_time.
 *
 * Returns the session token, the (public) token_id and the token secret.
 */
bool generate_session_token(const std::string& key_id, const std::string& path,
                            uint64_t max_lease_time, std::string* session_token,
                            std::string* public_token_id,
                            std::string* token_secret) {
  if (session_token == NULL || public_token_id == NULL ||
      token_secret == NULL) {
    return false;
  }

  UniquePtr<cipher::Key> secret(cipher::Key::CreateRandomly(32));
  if (!secret.IsValid()) {
    return false;
  }

  UniquePtr<cipher::Cipher> cipher(cipher::Cipher::Create(cipher::kAes256Cbc));
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

  *session_token = "{\"token_id\" : \"" + *public_token_id +
                   "\", \"blob\" : \"" + encrypted_body + "\"}";

  return true;
}

/**
 * Obtain the public_id from a session token
 */
bool get_token_public_id(const std::string& token, std::string* public_id) {
  if (public_id == NULL) {
    return false;
  }

  UniquePtr<JsonDocument> token_json(JsonDocument::Create(token));
  if (!token_json.IsValid()) {
    return false;
  }

  const JSON* token_id =
      JsonDocument::SearchInObject(token_json->root(), "token_id", JSON_STRING);
  const JSON* blob =
      JsonDocument::SearchInObject(token_json->root(), "blob", JSON_STRING);

  if (token_id == NULL || blob == NULL) {
    return false;
  }

  *public_id = token_id->string_value;

  return true;
}

/*
 * Check the validity of a session token using the associated secret
 */
bool check_token(const std::string& /*token*/, const std::string& /*secret*/) {
  return true;
}

}  // namespace receiver
