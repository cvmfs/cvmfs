/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "swissknife_lease.h"
#include "swissknife_lease_json.h"

class T_SwissknifeLeaseJson : public ::testing::Test {
 protected:
  void SetUp() { }

  CurlBuffer buffer_;
  std::string session_token_;
};

TEST_F(T_SwissknifeLeaseJson, ParseAcquireOk) {
  buffer_.data = "{ \"status\" : \"ok\", \"session_token\" : \"ABCD\"}";
  EXPECT_EQ(kLeaseReplySuccess, ParseAcquireReply(buffer_, &session_token_));
  EXPECT_EQ(session_token_, "ABCD");
}

TEST_F(T_SwissknifeLeaseJson, ParseAcquireBusy) {
  buffer_.data = "{ \"status\" : \"path_busy\", \"time_remaining\" : 1234 }";
  EXPECT_EQ(kLeaseReplyBusy, ParseAcquireReply(buffer_, &session_token_));
}

TEST_F(T_SwissknifeLeaseJson, ParseAcquireError) {
  buffer_
      .data = "{ \"status\" : \"error\", \"reason\" : \"reason_for_failure\" }";
  EXPECT_EQ(kLeaseReplyFailure, ParseAcquireReply(buffer_, &session_token_));
}

TEST_F(T_SwissknifeLeaseJson, ParseAcquireInvalidReply) {
  buffer_.data = "-----------------------------";
  EXPECT_EQ(kLeaseReplyFailure, ParseAcquireReply(buffer_, &session_token_));
}

TEST_F(T_SwissknifeLeaseJson, ParseAcquireNullPtr) {
  EXPECT_EQ(kLeaseReplyFailure, ParseAcquireReply(buffer_, NULL));
}

TEST_F(T_SwissknifeLeaseJson, ParseAcquireEmptyReply) {
  buffer_.data.clear();
  EXPECT_EQ(kLeaseReplyFailure, ParseAcquireReply(buffer_, &session_token_));
}

TEST_F(T_SwissknifeLeaseJson, ParseDropOk) {
  buffer_.data = "{ \"status\" : \"ok\" }";
  EXPECT_EQ(kLeaseReplySuccess, ParseDropReply(buffer_));
}

TEST_F(T_SwissknifeLeaseJson, ParseDropInvalidSessionToken) {
  buffer_.data = "{ \"status\" : \"invalid_token\" }";
  EXPECT_EQ(kLeaseReplyFailure, ParseDropReply(buffer_));
}

TEST_F(T_SwissknifeLeaseJson, ParseDropError) {
  buffer_
      .data = "{ \"status\" : \"error\", \"reason\" : \"reason_for_failure\" }";
  EXPECT_EQ(kLeaseReplyFailure, ParseDropReply(buffer_));
}

TEST_F(T_SwissknifeLeaseJson, ParseDropInvalidReply) {
  buffer_.data = "-----------------------------";
  EXPECT_EQ(kLeaseReplyFailure, ParseDropReply(buffer_));
}

TEST_F(T_SwissknifeLeaseJson, ParseDropEmptyReply) {
  buffer_.data.clear();
  EXPECT_EQ(kLeaseReplyFailure, ParseDropReply(buffer_));
}
