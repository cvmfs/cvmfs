/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <logging.h>
#include "json_document.h"
#include "pack.h"
#include "receiver/reactor.h"
#include "util/pointer.h"
#include "util/string.h"
#include "util_concurrency.h"

using namespace receiver;  // NOLINT

class MockedReactor : public Reactor {
 public:
  MockedReactor(int fdin, int fdout) : Reactor(fdin, fdout) {}
};

class T_Reactor : public ::testing::Test {
 protected:
  T_Reactor() : ready_(1, 1) {}

  virtual void SetUp() {
    ASSERT_NE(pipe(to_reactor_), -1);
    ASSERT_NE(pipe(from_reactor_), -1);

    ASSERT_EQ(pthread_create(&thread_, NULL, T_Reactor::ReactorFunction,
                             static_cast<void*>(this)),
              0);

    ASSERT_EQ(ready_.Dequeue(), true);
  }

  virtual void TearDown() {
    ASSERT_EQ(pthread_join(thread_, NULL), 0);
    close(to_reactor_[1]);
    close(from_reactor_[0]);
  }

  static void* ReactorFunction(void* data) {
    T_Reactor* ctx = static_cast<T_Reactor*>(data);

    ctx->ready_.Enqueue(true);

    MockedReactor reactor(ctx->to_reactor_[0], ctx->from_reactor_[1]);
    reactor.run();

    close(ctx->to_reactor_[0]);
    close(ctx->from_reactor_[1]);

    return NULL;
  }

  FifoChannel<bool> ready_;
  pthread_t thread_;
  int to_reactor_[2];
  int from_reactor_[2];
};

TEST_F(T_Reactor, kEcho_kQuit) {
  ASSERT_TRUE(Reactor::WriteRequest(to_reactor_[1], Reactor::kEcho, "Hey"));

  std::string reply;
  ASSERT_TRUE(Reactor::ReadReply(from_reactor_[0], &reply));
  ASSERT_EQ(reply, "Hey");

  ASSERT_TRUE(Reactor::WriteRequest(to_reactor_[1], Reactor::kQuit, ""));

  ASSERT_TRUE(Reactor::ReadReply(from_reactor_[0], &reply));
  ASSERT_EQ(reply, "ok");
}

TEST_F(T_Reactor, kGenerateToken_kQuit) {
  const std::string req_data =
      "{\"key_id\":\"some_key\",\"path\":\"some_path\",\"max_lease_time\":10}";

  ASSERT_TRUE(
      Reactor::WriteRequest(to_reactor_[1], Reactor::kGenerateToken, req_data));

  std::string reply;
  ASSERT_TRUE(Reactor::ReadReply(from_reactor_[0], &reply));

  UniquePtr<JsonDocument> json_reply(JsonDocument::Create(reply));
  ASSERT_TRUE(json_reply.IsValid());

  // Send kQuit request
  ASSERT_TRUE(Reactor::WriteRequest(to_reactor_[1], Reactor::kQuit, ""));
  reply.clear();
  ASSERT_TRUE(Reactor::ReadReply(from_reactor_[0], &reply));
  ASSERT_EQ(reply, "ok");
}

TEST_F(T_Reactor, FullCycle) {
  const std::string key_id = "some_key";
  const std::string path = "some_path";

  const std::string req_data = "{\"key_id\":\"" + key_id + "\",\"path\":\"" +
                               path + "\",\"max_lease_time\":10}";

  // Generate token
  ASSERT_TRUE(
      Reactor::WriteRequest(to_reactor_[1], Reactor::kGenerateToken, req_data));

  std::string reply;
  ASSERT_TRUE(Reactor::ReadReply(from_reactor_[0], &reply));

  std::string token, public_id, secret;
  {
    UniquePtr<JsonDocument> json_reply(JsonDocument::Create(reply));
    ASSERT_TRUE(json_reply.IsValid());

    // Extract the token, public_id and secret from the reply
    const JSON* token_json =
        JsonDocument::SearchInObject(json_reply->root(), "token", JSON_STRING);
    ASSERT_TRUE(token_json);
    token = token_json->string_value;

    const JSON* public_id_json =
        JsonDocument::SearchInObject(json_reply->root(), "id", JSON_STRING);
    ASSERT_TRUE(public_id_json);
    public_id = public_id_json->string_value;

    const JSON* secret_json =
        JsonDocument::SearchInObject(json_reply->root(), "secret", JSON_STRING);
    ASSERT_TRUE(secret_json);
    secret = secret_json->string_value;
  }

  // Get the public_id from the token
  ASSERT_TRUE(
      Reactor::WriteRequest(to_reactor_[1], Reactor::kGetTokenId, token));

  reply.clear();
  ASSERT_TRUE(Reactor::ReadReply(from_reactor_[0], &reply));
  {
    UniquePtr<JsonDocument> json_reply(JsonDocument::Create(reply));
    ASSERT_TRUE(json_reply.IsValid());

    // Extract the token, public_id and secret from the reply
    const JSON* id_json =
        JsonDocument::SearchInObject(json_reply->root(), "id", JSON_STRING);
    ASSERT_TRUE(id_json);
    ASSERT_EQ(id_json->string_value, public_id);
  }

  // Check the token validity
  json_string_input request_terms;
  request_terms.push_back(std::make_pair("token", &token[0]));
  request_terms.push_back(std::make_pair("secret", &secret[0]));

  std::string request;
  ASSERT_TRUE(ToJsonString(request_terms, &request));
  ASSERT_TRUE(
      Reactor::WriteRequest(to_reactor_[1], Reactor::kCheckToken, request));

  reply.clear();
  ASSERT_TRUE(Reactor::ReadReply(from_reactor_[0], &reply));
  {
    UniquePtr<JsonDocument> json_reply(JsonDocument::Create(reply));
    ASSERT_TRUE(json_reply.IsValid());

    // Extract the token, public_id and secret from the reply
    const JSON* path_json =
        JsonDocument::SearchInObject(json_reply->root(), "path", JSON_STRING);
    ASSERT_TRUE(path_json);
    ASSERT_EQ(path_json->string_value, path);
  }

  // Submit a payload
  {
    // Prepare an object pack and send it through the pipe
    ObjectPack pack;
    ObjectPack::BucketHandle hd = pack.NewBucket();

    std::vector<uint8_t> buffer(4096, 0);
    ObjectPack::AddToBucket(&buffer[0], 4096, hd);
    shash::Any buffer_hash(shash::kSha1);
    shash::HashMem(&buffer[0], buffer.size(), &buffer_hash);
    ASSERT_TRUE(pack.CommitBucket(ObjectPack::kCas, buffer_hash, hd));

    ObjectPackProducer serializer(&pack);

    shash::Any digest(shash::kSha1);
    serializer.GetDigest(&digest);

    const std::string request = "{\"path\":\"some_path\",\"digest\":\"" +
                                Base64(digest.ToString(false)) +
                                "\",\"header_size\":" +
                                StringifyInt(serializer.GetHeaderSize()) + "}";

    std::vector<unsigned char> payload(0);
    std::vector<unsigned char> buf(4096);
    unsigned nbytes = 0;
    do {
      nbytes = serializer.ProduceNext(buf.size(), &buf[0]);
      std::copy(buf.begin(), buf.begin() + nbytes, std::back_inserter(payload));
    } while (nbytes > 0);

    ASSERT_TRUE(Reactor::WriteRequest(to_reactor_[1], Reactor::kSubmitPayload,
                                      request));

    int nb = write(to_reactor_[1], &payload[0], payload.size());
    ASSERT_EQ(static_cast<size_t>(nb), payload.size());

    std::string reply;
    ASSERT_TRUE(Reactor::ReadReply(from_reactor_[0], &reply));
  }

  // Send kQuit request
  ASSERT_TRUE(Reactor::WriteRequest(to_reactor_[1], Reactor::kQuit, ""));
  reply.clear();
  ASSERT_TRUE(Reactor::ReadReply(from_reactor_[0], &reply));
  ASSERT_EQ(reply, "ok");
}
