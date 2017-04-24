/**
 * This file is part of the CernVM File System.
 */

#include "reactor.h"

#include <stdint.h>
#include <unistd.h>
#include <cstdlib>
#include <cstring>
#include <utility>
#include <vector>

#include "commit_processor.h"
#include "json_document.h"
#include "logging.h"
#include "payload_processor.h"
#include "session_token.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/string.h"

namespace receiver {

Reactor::Request Reactor::ReadRequest(int fd, std::string* data) {
  using namespace receiver;  // NOLINT

  // First, read the command identifier
  int32_t req_id = 0;
  int nb = SafeRead(fd, &req_id, 4);

  if (nb != 4) {
    return kError;
  }

  // Then, read message size
  int32_t msg_size = 0;
  nb = SafeRead(fd, &msg_size, 4);

  if (req_id == kError || nb != 4) {
    return kError;
  }

  // Finally read the message body
  if (msg_size > 0) {
    std::vector<char> buffer(msg_size);
    nb = SafeRead(fd, &buffer[0], msg_size);

    if (nb != msg_size) {
      return kError;
    }

    *data = std::string(&buffer[0], msg_size);
    return static_cast<Request>(req_id);
  }

  return kQuit;
}

bool Reactor::WriteRequest(int fd, Request req, const std::string& data) {
  const int32_t msg_size = data.size();
  const int32_t total_size = 8 + data.size();  // req + msg_size + data

  std::vector<char> buffer(total_size);

  memcpy(&buffer[0], &req, 4);
  memcpy(&buffer[4], &msg_size, 4);

  if (!data.empty()) {
    memcpy(&buffer[8], &data[0], data.size());
  }

  return SafeWrite(fd, &buffer[0], total_size);
}

bool Reactor::ReadReply(int fd, std::string* data) {
  int32_t msg_size(0);
  int nb = SafeRead(fd, &msg_size, 4);

  if (nb != 4) {
    return false;
  }

  std::vector<char> buffer(msg_size);
  nb = SafeRead(fd, &buffer[0], msg_size);

  if (nb != msg_size) {
    return false;
  }

  *data = std::string(&buffer[0], msg_size);

  return true;
}

bool Reactor::WriteReply(int fd, const std::string& data) {
  const int32_t msg_size = data.size();
  const int32_t total_size = 4 + data.size();

  std::vector<char> buffer(total_size);

  memcpy(&buffer[0], &msg_size, 4);

  if (!data.empty()) {
    memcpy(&buffer[4], &data[0], data.size());
  }

  return SafeWrite(fd, &buffer[0], total_size);
}

Reactor::Reactor(int fdin, int fdout) : fdin_(fdin), fdout_(fdout) {}

Reactor::~Reactor() {}

bool Reactor::Run() {
  std::string msg_body;
  Request req = kQuit;
  do {
    msg_body.clear();
    req = ReadRequest(fdin_, &msg_body);
    if (!HandleRequest(req, msg_body)) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "Reactor: could not handle request %d. Exiting", req);
      return false;
    }
  } while (req != kQuit);

  return true;
}

bool Reactor::HandleGenerateToken(const std::string& req, std::string* reply) {
  if (reply == NULL) {
    return false;
  }

  UniquePtr<JsonDocument> req_json(JsonDocument::Create(req));
  if (!req_json.IsValid()) {
    return false;
  }

  const JSON* key_id =
      JsonDocument::SearchInObject(req_json->root(), "key_id", JSON_STRING);
  const JSON* path =
      JsonDocument::SearchInObject(req_json->root(), "path", JSON_STRING);
  const JSON* max_lease_time = JsonDocument::SearchInObject(
      req_json->root(), "max_lease_time", JSON_INT);

  if (key_id == NULL || path == NULL || max_lease_time == NULL) {
    return false;
  }

  std::string session_token;
  std::string public_token_id;
  std::string token_secret;

  if (!GenerateSessionToken(key_id->string_value, path->string_value,
                            max_lease_time->int_value, &session_token,
                            &public_token_id, &token_secret)) {
    return false;
  }

  JsonStringInput input;
  input.push_back(std::make_pair("token", session_token.c_str()));
  input.push_back(std::make_pair("id", public_token_id.c_str()));
  input.push_back(std::make_pair("secret", token_secret.c_str()));

  ToJsonString(input, reply);

  return true;
}

bool Reactor::HandleGetTokenId(const std::string& req, std::string* reply) {
  if (reply == NULL) {
    return false;
  }

  std::string token_id;
  JsonStringInput input;
  if (!GetTokenPublicId(req, &token_id)) {
    input.push_back(std::make_pair("status", "error"));
    input.push_back(std::make_pair("reason", "invalid_token"));
  } else {
    input.push_back(std::make_pair("status", "ok"));
    input.push_back(std::make_pair("id", token_id.c_str()));
  }

  ToJsonString(input, reply);
  return true;
}

bool Reactor::HandleCheckToken(const std::string& req, std::string* reply) {
  if (reply == NULL) {
    return false;
  }

  UniquePtr<JsonDocument> req_json(JsonDocument::Create(req));
  if (!req_json.IsValid()) {
    return false;
  }

  const JSON* token =
      JsonDocument::SearchInObject(req_json->root(), "token", JSON_STRING);
  const JSON* secret =
      JsonDocument::SearchInObject(req_json->root(), "secret", JSON_STRING);

  if (token == NULL || secret == NULL) {
    return false;
  }

  std::string path;
  JsonStringInput input;
  TokenCheckResult ret =
      CheckToken(token->string_value, secret->string_value, &path);
  switch (ret) {
    case kExpired:
      // Expired token
      input.push_back(std::make_pair("status", "error"));
      input.push_back(std::make_pair("reason", "expired_token"));
      break;
    case kInvalid:
      // Invalid token
      input.push_back(std::make_pair("status", "error"));
      input.push_back(std::make_pair("reason", "invalid_token"));
      break;
    case kValid:
      // All ok
      input.push_back(std::make_pair("status", "ok"));
      input.push_back(std::make_pair("path", path.c_str()));
      break;
    default:
      // Should not be reached
      LogCvmfs(kLogCvmfs, kLogStderr,
               "Reactor::HandleCheckToken - Unknown value received. Exiting.");
      abort();
  }

  ToJsonString(input, reply);
  return true;
}

// This is a special handler. We need to continue reading the payload from the
// fdin_
bool Reactor::HandleSubmitPayload(int fdin, const std::string& req,
                                  std::string* reply) {
  if (!reply) {
    return false;
  }

  // Extract the Path (used for verification), Digest and DigestSize from the
  // request JSON.
  UniquePtr<JsonDocument> req_json(JsonDocument::Create(req));
  if (!req_json.IsValid()) {
    return false;
  }

  const JSON* path_json =
      JsonDocument::SearchInObject(req_json->root(), "path", JSON_STRING);
  const JSON* digest_json =
      JsonDocument::SearchInObject(req_json->root(), "digest", JSON_STRING);
  const JSON* header_size_json =
      JsonDocument::SearchInObject(req_json->root(), "header_size", JSON_INT);

  if (path_json == NULL || digest_json == NULL || header_size_json == NULL) {
    return false;
  }

  UniquePtr<PayloadProcessor> proc(MakePayloadProcessor());
  JsonStringInput reply_input;
  PayloadProcessor::Result res =
      proc->Process(fdin, digest_json->string_value, path_json->string_value,
                    header_size_json->int_value);

  switch (res) {
    case PayloadProcessor::kPathViolation:
      reply_input.push_back(std::make_pair("status", "error"));
      reply_input.push_back(std::make_pair("reason", "path_violation"));
      break;
    case PayloadProcessor::kOtherError:
      reply_input.push_back(std::make_pair("status", "error"));
      reply_input.push_back(std::make_pair("reason", "other_error"));
      break;
    case PayloadProcessor::kSuccess:
      reply_input.push_back(std::make_pair("status", "ok"));
      break;
    default:
      LogCvmfs(kLogCvmfs, kLogStderr,
               "Unknown value of PayloadProcessor::Result encountered.");
      abort();
      break;
  }

  ToJsonString(reply_input, reply);

  return true;
}

bool Reactor::HandleCommit(const std::string& req, std::string* reply) {
  if (!reply) {
    return false;
  }

  // Extract the Path from the request JSON.
  UniquePtr<JsonDocument> req_json(JsonDocument::Create(req));
  if (!req_json.IsValid()) {
    return false;
  }

  const JSON* path_json =
      JsonDocument::SearchInObject(req_json->root(), "path", JSON_STRING);

  // Here we use the path to commit the changes!
  UniquePtr<CommitProcessor> proc(MakeCommitProcessor());
  JsonStringInput reply_input;
  CommitProcessor::Result res = proc->Process(path_json->string_value);

  switch (res) {
    case CommitProcessor::kSuccess:
      reply_input.push_back(std::make_pair("status", "ok"));
      break;
    case CommitProcessor::kPathViolation:
      reply_input.push_back(std::make_pair("status", "error"));
      reply_input.push_back(std::make_pair("reason", "path_violation"));
    default:
      LogCvmfs(kLogCvmfs, kLogStderr,
               "Unknown value of CommitProcessor::Result encountered.");
      abort();
      break;
  }

  ToJsonString(reply_input, reply);

  return true;
}

PayloadProcessor* Reactor::MakePayloadProcessor() {
  return new PayloadProcessor();
}

CommitProcessor* Reactor::MakeCommitProcessor() {
  return new CommitProcessor();
}

bool Reactor::HandleRequest(Request req, const std::string& data) {
  bool ok = true;
  std::string reply;
  switch (req) {
    case kQuit:
      ok = WriteReply(fdout_, "ok");
      break;
    case kEcho:
      ok = WriteReply(fdout_, data);
      break;
    case kGenerateToken:
      ok &= HandleGenerateToken(data, &reply);
      ok &= WriteReply(fdout_, reply);
      break;
    case kGetTokenId:
      ok &= HandleGetTokenId(data, &reply);
      ok &= WriteReply(fdout_, reply);
      break;
    case kCheckToken:
      ok &= HandleCheckToken(data, &reply);
      ok &= WriteReply(fdout_, reply);
      break;
    case kSubmitPayload:
      ok &= HandleSubmitPayload(fdin_, data, &reply);
      ok &= WriteReply(fdout_, reply);
      break;
    case kCommit:
      ok &= HandleCommit(data, &reply);
      ok &= WriteReply(fdout_, reply);
      break;
    case kError:
      LogCvmfs(kLogCvmfs, kLogStderr, "Reactor: unknown command received.");
      ok = false;
      break;
    default:
      break;
  }

  return ok;
}

}  // namespace receiver
