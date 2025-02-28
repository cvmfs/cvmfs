/**
 * This file is part of the CernVM File System.
 */

#include "swissknife_lease_json.h"

#include <cstdint>
#include <string>

#include "json.h"
#include "json_document.h"
#include "swissknife_lease_curl.h"
#include "util/logging.h"
#include "util/pointer.h"

LeaseReply ParseAcquireReply(const CurlBuffer &buffer,
                             std::string *session_token, uint64_t *current_revision, std::string &current_root_hash) {
  if (buffer.data.size() == 0 || session_token == nullptr) {
    return kLeaseReplyFailure;
  }

  const UniquePtr<JsonDocument> reply(JsonDocument::Create(buffer.data));
  if (!reply.IsValid() || !reply->IsValid()) {
    return kLeaseReplyFailure;
  }

  const JSON *result =
      JsonDocument::SearchInObject(reply->root(), "status", JSON_STRING);
  if (result != nullptr) {
    const std::string status = result->string_value;
    if (status == "ok") {
      LogCvmfs(kLogCvmfs, kLogStdout, "Gateway reply: ok"); // NOLINT(misc-include-cleaner)
      const JSON *token = JsonDocument::SearchInObject(
          reply->root(), "session_token", JSON_STRING);
      if (token != nullptr) {
        const JSON *rev  = JsonDocument::SearchInObject(reply->root(), "revision", JSON_INT); //TODO(mharvey) FIXME: make the json lib uint64 aware
        if(rev!=nullptr) { *current_revision = static_cast<uint64_t>(rev->int_value); }
        const JSON *hash = JsonDocument::SearchInObject(reply->root(), "root_hash", JSON_STRING);
        if(hash!=nullptr) { current_root_hash = hash->string_value; }
        LogCvmfs(kLogCvmfs, kLogDebug, "Session token: %s", // NOLINT(misc-include-cleaner)
                 token->string_value);
        *session_token = token->string_value;
        return kLeaseReplySuccess;
      }
    } else if (status == "path_busy") {
      const JSON *time_remaining = JsonDocument::SearchInObject(
          reply->root(), "time_remaining", JSON_STRING);
      if (time_remaining != nullptr) {
        LogCvmfs(kLogCvmfs, kLogStdout, "Path busy. Time remaining = %s",
                 time_remaining->string_value);
        return kLeaseReplyBusy;
      }
    } else if (status == "error") {
      const JSON *reason =
          JsonDocument::SearchInObject(reply->root(), "reason", JSON_STRING);
      if (reason != nullptr) {
        LogCvmfs(kLogCvmfs, kLogStdout, "Error: %s", reason->string_value);
      }
    } else {
      LogCvmfs(kLogCvmfs, kLogStdout, "Unknown reply. Status: %s",
               status.c_str());
    }
  }

  return kLeaseReplyFailure;
}

LeaseReply ParseDropReply(const CurlBuffer &buffer) {
  if (buffer.data.size() == 0) {
    return kLeaseReplyFailure;
  }

  const UniquePtr<const JsonDocument> reply(JsonDocument::Create(buffer.data));
  if (!reply.IsValid() || !reply->IsValid()) {
    return kLeaseReplyFailure;
  }

  const JSON *result =
      JsonDocument::SearchInObject(reply->root(), "status", JSON_STRING);
  if (result != nullptr) {
    const std::string status = result->string_value;
    if (status == "ok") {
      LogCvmfs(kLogCvmfs, kLogStdout, "Gateway reply: ok");
      return kLeaseReplySuccess;
    } else if (status == "invalid_token") {
      LogCvmfs(kLogCvmfs, kLogStdout, "Error: invalid session token");
    } else if (status == "error") {
      const JSON *reason =
          JsonDocument::SearchInObject(reply->root(), "reason", JSON_STRING);
      if (reason != nullptr) {
        LogCvmfs(kLogCvmfs, kLogStdout, "Error: %s", reason->string_value);
      }
    } else {
      LogCvmfs(kLogCvmfs, kLogStdout, "Unknown reply. Status: %s",
               status.c_str());
    }
  }

  return kLeaseReplyFailure;
}
