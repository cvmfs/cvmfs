/**
 * This file is part of the CernVM File System.
 */

#include "swissknife_lease_json.h"

#include "util/pointer.h"
#include "json_document.h"
#include "json.h"

#include "logging.h"

bool ParseAcquireReply(const CurlBuffer& buffer, std::string& session_token) {
  //Parse reply
  UniquePtr<JsonDocument> reply(JsonDocument::Create(buffer.data));
  if (reply->IsValid()) {
    JSON* result = JsonDocument::SearchInObject(reply->root(), "status", JSON_STRING);
    if (result != NULL) {
      std::string status = result->string_value;
      if (status == "ok") {
        LogCvmfs(kLogCvmfs, kLogStderr, "Status: ok");
        JSON* token = JsonDocument::SearchInObject(reply->root(), "session_token", JSON_STRING);
        if (token != NULL) {
          LogCvmfs(kLogCvmfs, kLogStderr, "Session token: %s", token->string_value);
          session_token = token->string_value;
          return true;
        }
      } else if (status == "path_busy") {
        JSON* time_remaining = JsonDocument::SearchInObject(reply->root(), "time_remaining", JSON_INT);
        if (time_remaining != NULL) {
          LogCvmfs(kLogCvmfs, kLogStderr, "Path busy. Time remaining = %d", time_remaining->int_value);
        }
      } else if (status == "error") {
        JSON* reason = JsonDocument::SearchInObject(reply->root(), "reason", JSON_STRING);
        if (reason != NULL) {
          LogCvmfs(kLogCvmfs, kLogStderr, "Error: %s", reason->string_value);
        }
      } else {
        LogCvmfs(kLogCvmfs, kLogStderr, "Unknown reply. Status: %s", status.c_str());
      }
    }
  }

  return false;
}

bool ParseDropReply(const CurlBuffer& buffer) {
  UniquePtr<JsonDocument> reply(JsonDocument::Create(buffer.data));
  if (reply->IsValid()) {
    JSON* result = JsonDocument::SearchInObject(reply->root(), "status", JSON_STRING);
    if (result != NULL) {
      std::string status = result->string_value;
      if (status == "ok") {
        LogCvmfs(kLogCvmfs, kLogStderr, "Status: ok");
        return true;
      } else if (status == "invalid_token") {
        LogCvmfs(kLogCvmfs, kLogStderr, "Error: invalid session token");
      } else if (status == "error") {
        JSON* reason = JsonDocument::SearchInObject(reply->root(), "reason", JSON_STRING);
        if (reason != NULL) {
          LogCvmfs(kLogCvmfs, kLogStderr, "Error: %s", reason->string_value);
        }
      } else {
        LogCvmfs(kLogCvmfs, kLogStderr, "Unknown reply. Status: %s", status.c_str());
      }
    }
  }

  return false;
}

