/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>
#include <receiver/session_token.h>

using namespace receiver;  // NOLINT

class T_SessionToken : public ::testing::Test { };

TEST_F(T_SessionToken, GenerateBasic) {
  std::string session_token;
  std::string public_token_id;
  std::string token_secret;
  ASSERT_TRUE(GenerateSessionToken("some_key_id", "some_path", 10,
                                   &session_token, &public_token_id,
                                   &token_secret));
  ASSERT_FALSE(session_token.empty());
  ASSERT_FALSE(public_token_id.empty());
  ASSERT_FALSE(token_secret.empty());
}

TEST_F(T_SessionToken, GenerateWithInvalidParameters) {
  ASSERT_FALSE(
      GenerateSessionToken("key_id", "some_path", 10, NULL, NULL, NULL));
}

TEST_F(T_SessionToken, GetTokenId) {
  std::string session_token;
  std::string public_token_id;
  std::string token_secret;
  ASSERT_TRUE(GenerateSessionToken("some_key_id", "some_path", 10,
                                   &session_token, &public_token_id,
                                   &token_secret));
  std::string recovered_token_id;
  ASSERT_TRUE(GetTokenPublicId(session_token, &recovered_token_id));
  ASSERT_EQ(public_token_id, recovered_token_id);
}

TEST_F(T_SessionToken, CheckTokenSuccess) {
  std::string session_token;
  std::string public_token_id;
  std::string token_secret;
  ASSERT_TRUE(GenerateSessionToken("some_key_id", "some_path", 10,
                                   &session_token, &public_token_id,
                                   &token_secret));

  std::string path;
  ASSERT_EQ(CheckToken(session_token, token_secret, &path), kValid);
  ASSERT_EQ(path, "some_path");
}

TEST_F(T_SessionToken, CheckExpiredTokenSlow) {
  std::string session_token;
  std::string public_token_id;
  std::string token_secret;
  ASSERT_TRUE(GenerateSessionToken("some_key_id", "some_path", 0,
                                   &session_token, &public_token_id,
                                   &token_secret));

  sleep(1);

  std::string path;
  ASSERT_EQ(kExpired, CheckToken(session_token, token_secret, &path));
}
