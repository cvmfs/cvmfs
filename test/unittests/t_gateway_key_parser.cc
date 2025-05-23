/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "gateway_util.h"
#include "util/string.h"

class T_GatewayKeyParser : public ::testing::Test {
 protected:
  virtual void SetUp() { }
  virtual void TearDown() { }
};

TEST_F(T_GatewayKeyParser, ValidIdeal) {
  std::string key_id;
  std::string secret;
  ASSERT_TRUE(gateway::ParseKey("plain_text id secret", &key_id, &secret));
}

TEST_F(T_GatewayKeyParser, ValidMultipleSpaces) {
  std::string key_id;
  std::string secret;
  ASSERT_TRUE(
      gateway::ParseKey("    plain_text   id   secret    ", &key_id, &secret));
}

TEST_F(T_GatewayKeyParser, ValidTabs) {
  std::string key_id;
  std::string secret;
  ASSERT_TRUE(
      gateway::ParseKey("\tplain_text\tid\tsecret\t", &key_id, &secret));
}

TEST_F(T_GatewayKeyParser, ValidTabsSpacesAndNewlines) {
  std::string key_id;
  std::string secret;
  ASSERT_TRUE(gateway::ParseKey(" \t   plain_text \t  id \t  secret \t \n  ",
                                &key_id, &secret));
}

TEST_F(T_GatewayKeyParser, ValidRepeatingChars) {
  std::string key_id;
  std::string secret;
  ASSERT_TRUE(
      gateway::ParseKey("plain_text key111 sseeccrreett", &key_id, &secret));
  ASSERT_EQ("key111", key_id);
  ASSERT_EQ("sseeccrreett", secret);
}

TEST_F(T_GatewayKeyParser, InvalidTypo) {
  std::string key_id;
  std::string secret;
  ASSERT_FALSE(gateway::ParseKey("plane_text id secret", &key_id, &secret));
}

TEST_F(T_GatewayKeyParser, InvalidMissingSeparator) {
  std::string key_id;
  std::string secret;
  ASSERT_FALSE(gateway::ParseKey("plain_textid secret", &key_id, &secret));
}

TEST_F(T_GatewayKeyParser, InvalidTrailingGarbage) {
  std::string key_id;
  std::string secret;
  ASSERT_FALSE(
      gateway::ParseKey("plain_text id secret garbage", &key_id, &secret));
}
