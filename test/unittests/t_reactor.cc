/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <logging.h>
#include "json_document.h"
#include "receiver/reactor.h"
#include "util/pointer.h"
#include "util/string.h"
#include "util_concurrency.h"

using namespace receiver;  // NOLINT

class MockedReactor : public Reactor {
 public:
  MockedReactor(int fdin, int fdout) : Reactor(fdin, fdout) {}

 protected:
  virtual bool HandleRequest(int fdout, Request req, const std::string& data) {
    return Reactor::HandleRequest(fdout, req, data);
  }
};

class T_Reactor : public ::testing::Test {
 protected:
  virtual void SetUp() {
    ASSERT_NE(pipe(to_reactor_), -1);
    ASSERT_NE(pipe(from_reactor_), -1);

    Context ctx;
    ctx.fds[0] = to_reactor_[0];
    ctx.fds[1] = from_reactor_[1];

    ASSERT_EQ(pthread_create(&thread_, NULL, T_Reactor::ReactorFunction,
                             static_cast<void*>(&ctx)),
              0);

    ASSERT_EQ(ctx.ready.Dequeue(), true);
  }

  virtual void TearDown() {
    ASSERT_EQ(pthread_join(thread_, NULL), 0);
    close(to_reactor_[1]);
    close(from_reactor_[0]);
  }

  struct Context {
    Context() : fds(), ready(1, 1) {}

    int fds[2];
    FifoChannel<bool> ready;
  };

  static void* ReactorFunction(void* data) {
    Context* ctx = static_cast<Context*>(data);

    ctx->ready.Enqueue(true);

    MockedReactor reactor(ctx->fds[0], ctx->fds[1]);
    reactor.run();

    close(ctx->fds[0]);
    close(ctx->fds[1]);

    return NULL;
  }

  pthread_t thread_;
  int to_reactor_[2];
  int from_reactor_[2];
};

TEST_F(T_Reactor, kEcho_kQuit) {
  ASSERT_TRUE(Reactor::WriteRequest(to_reactor_[1], kEcho, "Hey"));

  std::string reply;
  ASSERT_TRUE(Reactor::ReadReply(from_reactor_[0], &reply));
  ASSERT_EQ(reply, "Hey");

  ASSERT_TRUE(Reactor::WriteRequest(to_reactor_[1], kQuit, ""));

  ASSERT_TRUE(Reactor::ReadReply(from_reactor_[0], &reply));
  ASSERT_EQ(reply, "ok");
}

TEST_F(T_Reactor, kGenerateToken_kQuit) {
  // Generate token
  json_string_input req_input;
  req_input.push_back(std::make_pair("key_id", "some_key"));
  req_input.push_back(std::make_pair("path", "some_path"));
  req_input.push_back(std::make_pair("max_lease_time", "10"));

  std::string req_data;
  ToJsonString(req_input, &req_data);

  ASSERT_TRUE(Reactor::WriteRequest(to_reactor_[1], kGenerateToken, req_data));

  std::string reply;
  ASSERT_TRUE(Reactor::ReadReply(from_reactor_[0], &reply));

  UniquePtr<JsonDocument> json_reply(JsonDocument::Create(reply));
  ASSERT_TRUE(json_reply.IsValid());

  // Send kQuit request
  ASSERT_TRUE(Reactor::WriteRequest(to_reactor_[1], kQuit, ""));
  reply.clear();
  ASSERT_TRUE(Reactor::ReadReply(from_reactor_[0], &reply));
  ASSERT_EQ(reply, "ok");
}

TEST_F(T_Reactor, FullCycle) {
  const std::string key_id = "some_key";
  const std::string path = "some_path";
  const std::string max_lease_time = "10";

  // Generate token
  json_string_input req_input;
  req_input.push_back(std::make_pair("key_id", &key_id[0]));
  req_input.push_back(std::make_pair("path", &path[0]));
  req_input.push_back(std::make_pair("max_lease_time", &max_lease_time[0]));

  std::string req_data;
  ToJsonString(req_input, &req_data);

  ASSERT_TRUE(Reactor::WriteRequest(to_reactor_[1], kGenerateToken, req_data));

  std::string reply;
  ASSERT_TRUE(Reactor::ReadReply(from_reactor_[0], &reply));

  UniquePtr<JsonDocument> json_reply(JsonDocument::Create(reply));
  ASSERT_TRUE(json_reply.IsValid());

  // Extract the token, public_id and secret from the reply
  const JSON* token_json =
      JsonDocument::SearchInObject(json_reply->root(), "token", JSON_STRING);
  ASSERT_TRUE(token_json);
  std::string token = token_json->string_value;

  const JSON* public_id_json =
      JsonDocument::SearchInObject(json_reply->root(), "id", JSON_STRING);
  ASSERT_TRUE(public_id_json);
  std::string public_id = public_id_json->string_value;

  const JSON* secret_json =
      JsonDocument::SearchInObject(json_reply->root(), "secret", JSON_STRING);
  ASSERT_TRUE(secret_json);
  std::string secret = secret_json->string_value;

  // Get the public_id from the token
  ASSERT_TRUE(Reactor::WriteRequest(to_reactor_[1], kGetTokenId, token));

  reply.clear();
  ASSERT_TRUE(Reactor::ReadReply(from_reactor_[0], &reply));
  ASSERT_EQ(reply, public_id);

  // Check the token validity
  json_string_input request_terms;
  request_terms.push_back(std::make_pair("token", &token[0]));
  request_terms.push_back(std::make_pair("secret", &secret[0]));

  std::string request;
  ASSERT_TRUE(ToJsonString(request_terms, &request));
  ASSERT_TRUE(Reactor::WriteRequest(to_reactor_[1], kCheckToken, request));

  reply.clear();
  ASSERT_TRUE(Reactor::ReadReply(from_reactor_[0], &reply));
  ASSERT_EQ(reply, path);

  // Send kQuit request
  ASSERT_TRUE(Reactor::WriteRequest(to_reactor_[1], kQuit, ""));
  reply.clear();
  ASSERT_TRUE(Reactor::ReadReply(from_reactor_[0], &reply));
  ASSERT_EQ(reply, "ok");
}
