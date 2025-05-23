/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "notify/messages.h"
#include "util/logging.h"
#include "util/string.h"

class T_Messages : public ::testing::Test { };

TEST_F(T_Messages, kPackUnpackActivity) {
  notify::msg::Activity msg_in;
  msg_in.version_ = 1;
  msg_in.timestamp_ = "now";
  msg_in.repository_ = "test.cern.ch";
  msg_in.manifest_ = "XSCXSCXXSZZZXSCXSC";

  std::string packed;
  msg_in.ToJSONString(&packed);

  notify::msg::Activity msg_out;
  EXPECT_TRUE(msg_out.FromJSONString(packed));

  EXPECT_EQ(msg_in, msg_out);
}

TEST_F(T_Messages, kIncompleteJSON) {
  std::string packed = "{ \"version\" : 1,\"type\" : \"activity\","
                       "\"repository\" : \"test.cern.ch\",\"manifest\" : \""
                       + Base64("XSCXSCXXSZZZXSCXSC") + "\"}";

  notify::msg::Activity msg_out;
  EXPECT_FALSE(msg_out.FromJSONString(packed));
}
