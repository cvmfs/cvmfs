/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <receiver/session_token.h>

using namespace receiver;  // NOLINT

class T_SessionToken : public ::testing::Test {};

TEST_F(T_SessionToken, GenerateBasic) {
  std::string session_token;
  std::string public_token_id;
  std::string token_secret;
  ASSERT_EQ(
      generate_session_token("some_key_id", "some_path", 1, &session_token,
                             &public_token_id, &token_secret),
      0);
  ASSERT_FALSE(session_token.empty());
  ASSERT_FALSE(public_token_id.empty());
  ASSERT_FALSE(token_secret.empty());
}

TEST_F(T_SessionToken, GenerateWithInvalidParameters) {
  ASSERT_EQ(generate_session_token("key_id", "some_path", 1, NULL, NULL, NULL),
            1);
}

TEST_F(T_SessionToken, GetTokenId) {
  std::string session_token;
  std::string public_token_id;
  std::string token_secret;
  ASSERT_EQ(
      generate_session_token("some_key_id", "some_path", 1, &session_token,
                             &public_token_id, &token_secret),
      0);
  std::string recovered_token_id;
  ASSERT_EQ(get_token_public_id(session_token, &recovered_token_id), 0);
  ASSERT_EQ(public_token_id, recovered_token_id);
}
