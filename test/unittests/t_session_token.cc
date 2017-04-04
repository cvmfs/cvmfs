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
  ASSERT_TRUE(generate_session_token("some_key_id", "some_path", 1,
                                     &session_token, &public_token_id,
                                     &token_secret));
  ASSERT_FALSE(session_token.empty());
  ASSERT_FALSE(public_token_id.empty());
  ASSERT_FALSE(token_secret.empty());
}
