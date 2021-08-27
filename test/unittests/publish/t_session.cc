/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "../c_http_server.h"
#include "c_repository.h"
#include "logging.h"
#include "publish/except.h"
#include "publish/repository.h"
#include "publish/settings.h"
#include "util/posix.h"

using namespace std;  // NOLINT

namespace publish {

class T_Session : public ::testing::Test {
 protected:
  virtual void SetUp() {
  }

 protected:
};


TEST_F(T_Session, Acquire) {
  ASSERT_TRUE(SafeWriteToFile("plain_text id secret", "gw_key", 0600));
  Publisher::Session::Settings settings;
  settings.service_endpoint = "http://localhost:4999/api/v1";
  settings.repo_path = "test.cvmfs.io/lease_path";
  settings.gw_key_path = "gw_key_not_available";
  settings.token_path = "token";
  settings.llvl = kLogNone;

  Publisher::Session session_incomplete(settings);

  try {
    session_incomplete.Acquire();
    EXPECT_TRUE(false);
  } catch (const EPublish &e) {
    EXPECT_EQ(EPublish::kFailGatewayKey, e.failure()) << e.what();
  }

  settings.gw_key_path = "gw_key";
  Publisher::Session session(settings);
  try {
    session.Acquire();
    EXPECT_TRUE(false);
  } catch (const EPublish &e) {
    EXPECT_EQ(EPublish::kFailLeaseHttp, e.failure()) << e.what();
  }

  MockGateway gateway(4999);
  HTTPResponse next_response;
  gateway.next_response_.code = 404;
  gateway.next_response_.reason = "Not Found";
  try {
    session.Acquire();
    EXPECT_TRUE(false);
  } catch (const EPublish &e) {
    EXPECT_EQ(EPublish::kFailLeaseBody, e.failure()) << e.what();
  }

  gateway.next_response_.code = 200;
  gateway.next_response_.reason = "OK";
  gateway.next_response_.body =
    "{ \"status\" : \"error\", \"reason\" : \"XXX\"}";
  try {
    session.Acquire();
    EXPECT_TRUE(false);
  } catch (const EPublish &e) {
    EXPECT_EQ(EPublish::kFailLeaseBody, e.failure()) << e.what();
  }

  gateway.next_response_.body =
    "{ \"status\" : \"path_busy\", \"time_remaining\" : 42}";
  try {
    session.Acquire();
    EXPECT_TRUE(false);
  } catch (const EPublish &e) {
    EXPECT_EQ(EPublish::kFailLeaseBusy, e.failure()) << e.what();
  }

  gateway.next_response_.body =
    "{ \"status\" : \"ok\", \"session_token\" : \"ABC\" }";
  try {
    session.Acquire();
    session.SetKeepAlive(true);
  } catch (...) {
    EXPECT_TRUE(false);
  }
}


TEST_F(T_Session, Drop) {
  ASSERT_TRUE(SafeWriteToFile("plain_text id secret", "gw_key", 0600));
  Publisher::Session::Settings settings;
  settings.service_endpoint = "http://localhost:4999/api/v1";
  settings.repo_path = "test.cvmfs.io/lease_path";
  settings.gw_key_path = "gw_key";
  settings.token_path = "token_drop";
  settings.llvl = kLogNone;

  EXPECT_FALSE(FileExists("token_drop"));

  {
    Publisher::Session session(settings);
    EXPECT_FALSE(session.has_lease());
    try {
      session.Drop();
    } catch (...) {
      EXPECT_TRUE(false);
    }
  }

  CreateFile("token_drop", 0600);
  Publisher::Session session(settings);
  EXPECT_TRUE(session.has_lease());

  try {
    session.Drop();
    EXPECT_TRUE(false);
  } catch (const EPublish &e) {
    EXPECT_EQ(EPublish::kFailLeaseHttp, e.failure()) << e.what();
  }

  MockGateway gateway(4999);
  HTTPResponse next_response;
  gateway.next_response_.code = 404;
  gateway.next_response_.reason = "Not Found";
  try {
    session.Drop();
    EXPECT_TRUE(false);
  } catch (const EPublish &e) {
    EXPECT_EQ(EPublish::kFailLeaseBody, e.failure()) << e.what();
  }

  gateway.next_response_.code = 200;
  gateway.next_response_.reason = "OK";
  gateway.next_response_.body =
    "{ \"status\" : \"error\", \"reason\" : \"XXX\"}";
  try {
    session.Drop();
    EXPECT_TRUE(false);
  } catch (const EPublish &e) {
    EXPECT_EQ(EPublish::kFailLeaseBody, e.failure()) << e.what();
  }

  gateway.next_response_.body =
    "{ \"status\" : \"ok\" }";
  try {
    session.Drop();
  } catch (...) {
    EXPECT_TRUE(false);
  }

  EXPECT_FALSE(session.has_lease());
  EXPECT_FALSE(FileExists("token_drop"));
}

}  // namespace publish
