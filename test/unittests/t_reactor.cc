/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <logging.h>
#include "json_document.h"
#include "receiver/reactor.h"
#include "util/pointer.h"
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

TEST_F(T_Reactor, kQuit) {
  ASSERT_TRUE(Reactor::WriteRequest(to_reactor_[1], kQuit, ""));
  std::string reply;
  ASSERT_TRUE(Reactor::ReadReply(from_reactor_[0], &reply));
  ASSERT_EQ(reply, "ok");
}

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

  ASSERT_TRUE(Reactor::WriteRequest(to_reactor_[1], kQuit, ""));
  reply.clear();
  ASSERT_TRUE(Reactor::ReadReply(from_reactor_[0], &reply));
  ASSERT_EQ(reply, "ok");
}
