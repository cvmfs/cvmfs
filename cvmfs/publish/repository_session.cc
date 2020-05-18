/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "publish/repository.h"

#include <cassert>
#include <string>

#include "backoff.h"
#include "catalog_mgr_ro.h"
#include "directory_entry.h"
#include "duplex_curl.h"
#include "gateway_util.h"
#include "hash.h"
#include "json_document.h"
#include "logging.h"
#include "publish/except.h"
#include "upload.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/string.h"

namespace {

struct CurlBuffer {
  std::string data;
};

enum LeaseReply {
  kLeaseReplySuccess,
  kLeaseReplyBusy,
  kLeaseReplyFailure
};

static CURL* PrepareCurl(const std::string& method) {
  const char* user_agent_string = "cvmfs/" VERSION;

  CURL* h_curl = curl_easy_init();
  assert(h_curl != NULL);

  curl_easy_setopt(h_curl, CURLOPT_NOPROGRESS, 1L);
  curl_easy_setopt(h_curl, CURLOPT_USERAGENT, user_agent_string);
  curl_easy_setopt(h_curl, CURLOPT_MAXREDIRS, 50L);
  curl_easy_setopt(h_curl, CURLOPT_CUSTOMREQUEST, method.c_str());

  return h_curl;
}

static size_t RecvCB(void* buffer, size_t size, size_t nmemb, void* userp) {
  CurlBuffer* my_buffer = static_cast<CurlBuffer*>(userp);

  if (size * nmemb < 1) {
    return 0;
  }

  my_buffer->data = static_cast<char*>(buffer);

  return my_buffer->data.size();
}

static void MakeAcquireRequest(
  const gateway::GatewayKey &key,
  const std::string& repo_path,
  const std::string& repo_service_url,
  int llvl,
  CurlBuffer* buffer)
{
  CURLcode ret = static_cast<CURLcode>(0);

  CURL* h_curl = PrepareCurl("POST");

  const std::string payload = "{\"path\" : \"" + repo_path +
                              "\", \"api_version\" : \"" +
                              StringifyInt(gateway::APIVersion()) + "\"}";

  shash::Any hmac(shash::kSha1);
  shash::HmacString(key.secret(), payload, &hmac);

  const std::string header_str =
    std::string("Authorization: ") + key.id() + " " +
    Base64(hmac.ToString(false));
  struct curl_slist* auth_header = NULL;
  auth_header = curl_slist_append(auth_header, header_str.c_str());
  curl_easy_setopt(h_curl, CURLOPT_HTTPHEADER, auth_header);

  // Make request to acquire lease from repo services
  curl_easy_setopt(h_curl, CURLOPT_URL, (repo_service_url + "/leases").c_str());
  curl_easy_setopt(h_curl, CURLOPT_POSTFIELDSIZE_LARGE,
                   static_cast<curl_off_t>(payload.length()));
  curl_easy_setopt(h_curl, CURLOPT_POSTFIELDS, payload.c_str());
  curl_easy_setopt(h_curl, CURLOPT_WRITEFUNCTION, RecvCB);
  curl_easy_setopt(h_curl, CURLOPT_WRITEDATA, buffer);

  ret = curl_easy_perform(h_curl);
  curl_easy_cleanup(h_curl);
  if (ret != CURLE_OK) {
    LogCvmfs(kLogUploadGateway, llvl | kLogStderr,
             "Make lease acquire request failed: %d. Reply: %s", ret,
             buffer->data.c_str());
    throw publish::EPublish("cannot acquire lease",
                            publish::EPublish::kFailLeaseHttp);
  }
}

static LeaseReply ParseAcquireReply(
  const CurlBuffer &buffer,
  std::string *session_token,
  int llvl)
{
  if (buffer.data.size() == 0 || session_token == NULL) {
    return kLeaseReplyFailure;
  }

  const UniquePtr<JsonDocument> reply(JsonDocument::Create(buffer.data));
  if (!reply || !reply->IsValid()) {
    return kLeaseReplyFailure;
  }

  const JSON *result =
      JsonDocument::SearchInObject(reply->root(), "status", JSON_STRING);
  if (result != NULL) {
    const std::string status = result->string_value;
    if (status == "ok") {
      LogCvmfs(kLogCvmfs, llvl | kLogStdout, "Gateway reply: ok");
      const JSON *token = JsonDocument::SearchInObject(
          reply->root(), "session_token", JSON_STRING);
      if (token != NULL) {
        LogCvmfs(kLogCvmfs, kLogDebug, "Session token: %s",
                 token->string_value);
        *session_token = token->string_value;
        return kLeaseReplySuccess;
      }
    } else if (status == "path_busy") {
      const JSON *time_remaining = JsonDocument::SearchInObject(
          reply->root(), "time_remaining", JSON_STRING);
      LogCvmfs(kLogCvmfs, llvl | kLogStdout,
               "Path busy. Time remaining = %s", (time_remaining != NULL) ?
               time_remaining->string_value : "UNKNOWN");
      return kLeaseReplyBusy;
    } else if (status == "error") {
      const JSON *reason =
          JsonDocument::SearchInObject(reply->root(), "reason", JSON_STRING);
      if (reason != NULL) {
        LogCvmfs(kLogCvmfs, llvl | kLogStdout, "Error: %s",
                 reason->string_value);
      }
    } else {
      LogCvmfs(kLogCvmfs, llvl | kLogStdout, "Unknown reply. Status: %s",
               status.c_str());
    }
  }

  return kLeaseReplyFailure;
}

}  // anonymous namespace

namespace publish {

Publisher::Session::Session(const Settings &settings_session)
  : settings_(settings_session)
{
}


Publisher::Session *Publisher::Session::Create(
  const Settings &settings_session)
{
  Session *session = new Session(settings_session);
  session->Acquire();
  return session;
}


Publisher::Session *Publisher::Session::Create(
  const SettingsPublisher &settings_publisher,
  int llvl)
{
  // For all but the gateway storage type, acquiring session tokens is currently
  // a no-op.
  if (settings_publisher.storage().type() != upload::SpoolerDefinition::Gateway)
    return NULL;

  Settings settings_session;
  settings_session.service_endpoint = settings_publisher.storage().endpoint();
  settings_session.repo_path = settings_publisher.fqrn() + "/" +
                               settings_publisher.transaction().lease_path();
  settings_session.gw_key_path = settings_publisher.keychain().gw_key_path();
  settings_session.token_path =
    settings_publisher.transaction().spool_area().gw_session_token();
  settings_session.llvl = llvl;
  return Create(settings_session);
}


void Publisher::Session::Acquire() {
  gateway::GatewayKey gw_key = gateway::ReadGatewayKey(settings_.gw_key_path);
  if (!gw_key.IsValid()) {
    throw EPublish("cannot read gateway key: " + settings_.gw_key_path,
                   EPublish::kFailGatewayKey);
  }
  CurlBuffer buffer;
  MakeAcquireRequest(gw_key, settings_.repo_path, settings_.service_endpoint,
                     settings_.llvl, &buffer);

  std::string session_token;
  LeaseReply rep = ParseAcquireReply(buffer, &session_token, settings_.llvl);
  switch (rep) {
    case kLeaseReplySuccess:
      {
        bool rvb = SafeWriteToFile(
          session_token,
          settings_.token_path,
          0600);
        if (!rvb) {
          throw EPublish("cannot write session token: " + settings_.token_path);
        }
      }
      break;
    case kLeaseReplyBusy:
      throw EPublish("lease path busy", EPublish::kFailLeaseBusy);
      break;
    case kLeaseReplyFailure:
    default:
      throw EPublish("cannot parse session token", EPublish::kFailLeaseBody);
  }
}

Publisher::Session::~Session() {
  // TODO(jblomer): drop lease
}

}  // namespace publish
