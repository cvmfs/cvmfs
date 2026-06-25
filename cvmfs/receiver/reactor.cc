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
#include "json_document_write.h"
#include "payload_processor.h"
#include "repository_tag.h"
#include "session_token.h"
#include "upload_facility.h"
#include "util/exception.h"
#include "util/logging.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/string.h"

namespace receiver {

// NOTE, during the handling of the messages between the gateway and the
// receiver, we keep reading `4` bytes instead of the more common
// `sizeof(req_id)` or `sizeof(int32_t)`.
// This mirror well the behaviour of the gateway code.
// It would be possible on both codebase to ask the size of the type, but then
// we would need to make sure that the types are actually the same.
// It is simpler to send `4` bytes.

Reactor::Request Reactor::ReadRequest(int fd, std::string *data) {
  using namespace receiver;  // NOLINT

  // First, read the command identifier
  int32_t req_id = kQuit;
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
  }

  return static_cast<Request>(req_id);
}

bool Reactor::WriteRequest(int fd, Request req, const std::string &data) {
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

bool Reactor::ReadReply(int fd, std::string *data) {
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

bool Reactor::WriteReply(int fd, const std::string &data) {
  const int32_t msg_size = data.size();
  const int32_t total_size = 4 + data.size();

  std::vector<char> buffer(total_size);

  memcpy(&buffer[0], &msg_size, 4);

  if (!data.empty()) {
    memcpy(&buffer[4], &data[0], data.size());
  }

  return SafeWrite(fd, &buffer[0], total_size);
}

bool Reactor::ExtractStatsFromReq(JsonDocument *req, perf::Statistics *stats,
                                  std::string *start_time) {
  const perf::StatisticsTemplate stats_tmpl("publish", stats);
  const upload::UploadCounters counters(stats_tmpl);

  const JSON *statistics = JsonDocument::SearchInObject(
      req->root(), "statistics", JSON_OBJECT);
  if (statistics == NULL) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "Could not find 'statistics' field in request");
    return false;
  }

  const JSON *publish_ctrs = JsonDocument::SearchInObject(statistics, "publish",
                                                          JSON_OBJECT);

  if (publish_ctrs == NULL) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "Could not find 'statistics.publish' field in request");
    return false;
  }

  const JSON *n_chunks_added = JsonDocument::SearchInObject(
      publish_ctrs, "n_chunks_added", JSON_INT);
  const JSON *n_chunks_duplicated = JsonDocument::SearchInObject(
      publish_ctrs, "n_chunks_duplicated", JSON_INT);
  const JSON *n_catalogs_added = JsonDocument::SearchInObject(
      publish_ctrs, "n_catalogs_added", JSON_INT);
  const JSON *sz_uploaded_bytes = JsonDocument::SearchInObject(
      publish_ctrs, "sz_uploaded_bytes", JSON_INT);
  const JSON *sz_uploaded_catalog_bytes = JsonDocument::SearchInObject(
      publish_ctrs, "sz_uploaded_catalog_bytes", JSON_INT);

  const JSON *start_time_json = JsonDocument::SearchInObject(
      statistics, "start_time", JSON_STRING);

  if (n_chunks_added == NULL || n_chunks_duplicated == NULL
      || n_catalogs_added == NULL || sz_uploaded_bytes == NULL
      || sz_uploaded_catalog_bytes == NULL || start_time_json == NULL) {
    return false;
  }

  perf::Xadd(counters.n_chunks_added, n_chunks_added->get<int>());
  perf::Xadd(counters.n_chunks_duplicated, n_chunks_duplicated->get<int>());
  perf::Xadd(counters.n_catalogs_added, n_catalogs_added->get<int>());
  perf::Xadd(counters.sz_uploaded_bytes, sz_uploaded_bytes->get<int>());
  perf::Xadd(counters.sz_uploaded_catalog_bytes,
             sz_uploaded_catalog_bytes->get<int>());

  *start_time = start_time_json->get<std::string>();

  return true;
}

Reactor::Reactor(int fdin, int fdout) : fdin_(fdin), fdout_(fdout) { }

Reactor::~Reactor() { }

bool Reactor::Run() {
  std::string msg_body;
  Request req = kQuit;
  do {
    msg_body.clear();
    req = ReadRequest(fdin_, &msg_body);
    if (!HandleRequest(req, msg_body)) {
      LogCvmfs(kLogReceiver, kLogSyslogErr,
               "Reactor: could not handle request %d. Exiting", req);
      return false;
    }
  } while (req != kQuit);

  return true;
}

bool Reactor::HandleGenerateToken(const std::string &req, std::string *reply) {
  if (reply == NULL) {
    PANIC(kLogSyslogErr, "HandleGenerateToken: Invalid reply pointer.");
  }
  const UniquePtr<JsonDocument> req_json(JsonDocument::Create(req));
  if (!req_json.IsValid()) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "HandleGenerateToken: Invalid JSON request.");
    return false;
  }

  const JSON *key_id = JsonDocument::SearchInObject(req_json->root(), "key_id",
                                                    JSON_STRING);
  const JSON *path = JsonDocument::SearchInObject(req_json->root(), "path",
                                                  JSON_STRING);
  const JSON *max_lease_time = JsonDocument::SearchInObject(
      req_json->root(), "max_lease_time", JSON_INT);

  if (key_id == NULL || path == NULL || max_lease_time == NULL) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "HandleGenerateToken: Missing fields in request.");
    return false;
  }

  std::string session_token;
  std::string public_token_id;
  std::string token_secret;

  if (!GenerateSessionToken(key_id->get<std::string>(),
                            path->get<std::string>(),
                            max_lease_time->get<int>(), &session_token,
                            &public_token_id, &token_secret)) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "HandleGenerateToken: Could not generate session token.");
    return false;
  }

  JsonStringGenerator input;
  input.Add("token", session_token);
  input.Add("id", public_token_id);
  input.Add("secret", token_secret);
  const std::string json = input.GenerateString();
  *reply = json;

  return true;
}

bool Reactor::HandleGetTokenId(const std::string &req, std::string *reply) {
  if (reply == NULL) {
    PANIC(kLogSyslogErr, "HandleGetTokenId: Invalid reply pointer.");
  }

  std::string token_id;
  JsonStringGenerator input;
  if (!GetTokenPublicId(req, &token_id)) {
    input.Add("status", "error");
    input.Add("reason", "invalid_token");
  } else {
    input.Add("status", "ok");
    input.Add("id", token_id);
  }
  const std::string json = input.GenerateString();
  *reply = json;

  return true;
}

bool Reactor::HandleCheckToken(const std::string &req, std::string *reply) {
  if (reply == NULL) {
    PANIC(kLogSyslogErr, "HandleCheckToken: Invalid reply pointer.");
  }

  const UniquePtr<JsonDocument> req_json(JsonDocument::Create(req));
  if (!req_json.IsValid()) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "HandleCheckToken: Invalid JSON request.");
    return false;
  }

  const JSON *token = JsonDocument::SearchInObject(req_json->root(), "token",
                                                   JSON_STRING);
  const JSON *secret = JsonDocument::SearchInObject(req_json->root(), "secret",
                                                    JSON_STRING);

  if (token == NULL || secret == NULL) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "HandleCheckToken: Missing fields in request.");
    return false;
  }

  std::string path;
  JsonStringGenerator input;
  const TokenCheckResult ret = CheckToken(token->get<std::string>(),
                                          secret->get<std::string>(), &path);
  switch (ret) {
    case kExpired:
      // Expired token
      input.Add("status", "error");
      input.Add("reason", "expired_token");
      break;
    case kInvalid:
      // Invalid token
      input.Add("status", "error");
      input.Add("reason", "invalid_token");
      break;
    case kValid:
      // All ok
      input.Add("status", "ok");
      input.Add("path", path);
      break;
    default:
      // Should not be reached
      PANIC(kLogSyslogErr,
            "HandleCheckToken: Unknown value received. Exiting.");
  }

  const std::string json = input.GenerateString();
  *reply = json;

  return true;
}

// This is a special handler. We need to continue reading the payload from the
// fdin_
bool Reactor::HandleSubmitPayload(int fdin, const std::string &req,
                                  std::string *reply) {
  if (!reply) {
    PANIC(kLogSyslogErr, "HandleSubmitPayload: Invalid reply pointer.");
  }

  // Extract the Path (used for verification), Digest and DigestSize from the
  // request JSON.
  const UniquePtr<JsonDocument> req_json(JsonDocument::Create(req));
  if (!req_json.IsValid()) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "HandleSubmitPayload: Invalid JSON request.");
    return false;
  }

  const JSON *path_json = JsonDocument::SearchInObject(req_json->root(), "path",
                                                       JSON_STRING);
  const JSON *digest_json = JsonDocument::SearchInObject(req_json->root(),
                                                         "digest", JSON_STRING);
  const JSON *header_size_json = JsonDocument::SearchInObject(
      req_json->root(), "header_size", JSON_INT);

  if (path_json == NULL || digest_json == NULL || header_size_json == NULL) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "HandleSubmitPayload: Missing fields in request.");
    return false;
  }

  perf::Statistics statistics;

  const UniquePtr<PayloadProcessor> proc(MakePayloadProcessor());
  proc->SetStatistics(&statistics);
  JsonStringGenerator reply_input;
  const PayloadProcessor::Result res = proc->Process(
      fdin, digest_json->get<std::string>(), path_json->get<std::string>(),
      header_size_json->get<int>());

  switch (res) {
    case PayloadProcessor::kPathViolation:
      reply_input.Add("status", "error");
      reply_input.Add("reason", "path_violation");
      break;
    case PayloadProcessor::kOtherError:
      reply_input.Add("status", "error");
      reply_input.Add("reason", "other_error");
      break;
    case PayloadProcessor::kUploaderError:
      reply_input.Add("status", "error");
      reply_input.Add("reason", "uploader_error");
      break;
    case PayloadProcessor::kSuccess:
      reply_input.Add("status", "ok");
      break;
    default:
      PANIC(kLogSyslogErr,
            "HandleSubmitPayload: Unknown value of PayloadProcessor::Result "
            "encountered.");
      break;
  }

  // HandleSubmitPayload sends partial statistics back to the gateway
  const std::string stats_json = statistics.PrintJSON();
  reply_input.AddJsonObject("statistics", stats_json);

  const std::string json = reply_input.GenerateString();
  *reply = json;

  return true;
}

bool Reactor::HandleCommit(const std::string &req, std::string *reply) {
  return DoCommit(req, reply, /* direct_graft = */ false);
}

bool Reactor::HandleCommitGraft(const std::string &req, std::string *reply) {
  return DoCommit(req, reply, /* direct_graft = */ true);
}

// Shared body for kCommit and the experimental kCommitGraft.  The request
// payload is identical for both; direct_graft selects the DirectGraft fast path
// in CommitProcessor.
bool Reactor::DoCommit(const std::string &req, std::string *reply,
                       bool direct_graft) {
  if (!reply) {
    PANIC(kLogSyslogErr, "HandleCommit: Invalid reply pointer.");
  }
  // Extract the Path from the request JSON.
  const UniquePtr<JsonDocument> req_json(JsonDocument::Create(req));
  if (!req_json.IsValid()) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "HandleCommit: Invalid JSON request.");
    return false;
  }

  const JSON *lease_path_json = JsonDocument::SearchInObject(
      req_json->root(), "lease_path", JSON_STRING);
  const JSON *old_root_hash_json = JsonDocument::SearchInObject(
      req_json->root(), "old_root_hash", JSON_STRING);
  const JSON *new_root_hash_json = JsonDocument::SearchInObject(
      req_json->root(), "new_root_hash", JSON_STRING);
  const JSON *tag_name_json = JsonDocument::SearchInObject(
      req_json->root(), "tag_name", JSON_STRING);
  const JSON *tag_description_json = JsonDocument::SearchInObject(
      req_json->root(), "tag_description", JSON_STRING);
  const JSON *auto_tag_threshold_json = JsonDocument::SearchInObject(
      req_json->root(), "auto_tag_threshold", JSON_INT);
  const JSON *delete_tags_json = JsonDocument::SearchInObject(
      req_json->root(), "delete_tags", JSON_STRING);
  const JSON *lease_expiration_json = JsonDocument::SearchInObject(
      req_json->root(), "lease_expiration", JSON_INT);

  if (lease_path_json == NULL || old_root_hash_json == NULL
      || new_root_hash_json == NULL || lease_expiration_json == NULL) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "HandleCommit: Missing fields in request.");
    return false;
  }

  perf::Statistics statistics;
  std::string start_time;
  if (!Reactor::ExtractStatsFromReq(req_json.weak_ref(), &statistics,
                                    &start_time)) {
    LogCvmfs(
        kLogReceiver, kLogSyslogErr,
        "HandleCommit: Could not extract statistics counters from request");
  }
  uint64_t final_revision;

  // Here we use the path to commit the changes!
  const UniquePtr<CommitProcessor> proc(MakeCommitProcessor());
  proc->SetStatistics(&statistics, start_time);
  const shash::Any old_root_hash = shash::MkFromSuffixedHexPtr(
      shash::HexPtr(old_root_hash_json->get<std::string>()));
  const shash::Any new_root_hash = shash::MkFromSuffixedHexPtr(
      shash::HexPtr(new_root_hash_json->get<std::string>()));
  RepositoryTag repo_tag(tag_name_json->get<std::string>(),
                         tag_description_json->get<std::string>());
  if (auto_tag_threshold_json != NULL) {
    repo_tag.SetAutoTagThreshold(
        static_cast<time_t>(auto_tag_threshold_json->get<int64_t>()));
  }
  if (delete_tags_json != NULL) {
    repo_tag.SetDeleteTags(delete_tags_json->get<std::string>());
  }
  const CommitProcessor::Result res = proc->Process(
      lease_path_json->get<std::string>(), old_root_hash, new_root_hash,
      repo_tag, lease_expiration_json->get<int64_t>(), &final_revision,
      direct_graft);

  JsonStringGenerator reply_input;
  switch (res) {
    case CommitProcessor::kSuccess:
      reply_input.Add("status", "ok");
      reply_input.Add("final_revision", static_cast<int64_t>(final_revision));
      break;
    case CommitProcessor::kError:
      reply_input.Add("status", "error");
      reply_input.Add("reason", "miscellaneous");
      break;
    case CommitProcessor::kMergeFailure:
      reply_input.Add("status", "error");
      reply_input.Add("reason", "merge_error");
      break;
    case CommitProcessor::kMissingReflog:
      reply_input.Add("status", "error");
      reply_input.Add("reason", "missing_reflog");
      break;
    case CommitProcessor::kLeaseExpired:
      reply_input.Add("status", "error");
      reply_input.Add("reason", "lease_expired");
      break;
    default:
      PANIC(kLogSyslogErr,
            "Unknown value of CommitProcessor::Result encountered.");
      break;
  }

  const std::string json = reply_input.GenerateString();
  *reply = json;

  return true;
}

PayloadProcessor *Reactor::MakePayloadProcessor() {
  return new PayloadProcessor();
}

CommitProcessor *Reactor::MakeCommitProcessor() {
  return new CommitProcessor();
}

bool Reactor::HandleRequest(Request req, const std::string &data) {
  bool ok = true;
  std::string reply;
  try {
    switch (req) {
      case kQuit:
        ok = WriteReply(fdout_, "ok");
        break;
      case kEcho:
        ok = WriteReply(fdout_, std::string("PID: ") + StringifyUint(getpid()));
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
      case kCommitGraft:
        ok &= HandleCommitGraft(data, &reply);
        ok &= WriteReply(fdout_, reply);
        break;
      case kTestCrash:
        PANIC(kLogSyslogErr,
              "Crash for test purposes. Should never happen in production "
              "environment.");
        break;
      case kError:
        LogCvmfs(kLogReceiver, kLogSyslogErr,
                 "Reactor: unknown command received.");
        ok = false;
        break;
      default:
        break;
    }
  } catch (const ECvmfsException &e) {
    reply.clear();

    std::string error("runtime error: ");
    error += e.what();

    JsonStringGenerator input;
    input.Add("status", "error");
    input.Add("reason", error);

    reply = input.GenerateString();
    WriteReply(fdout_, reply);
    throw e;
  }

  return ok;
}

}  // namespace receiver
