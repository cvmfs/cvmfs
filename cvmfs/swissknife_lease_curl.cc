/**
 * This file is part of the CernVM File System.
 */

#include "swissknife_lease_curl.h"

#include "cvmfs_config.h"

#include "gateway_util.h"
#include "hash.h"
#include "logging.h"
#include "util/string.h"

namespace {

CURL* PrepareCurl(const std::string& method) {
  const char* user_agent_string = "cvmfs/" VERSION;

  CURL* h_curl = curl_easy_init();

  if (h_curl) {
    curl_easy_setopt(h_curl, CURLOPT_NOPROGRESS, 1L);
    curl_easy_setopt(h_curl, CURLOPT_USERAGENT, user_agent_string);
    curl_easy_setopt(h_curl, CURLOPT_MAXREDIRS, 50L);
    curl_easy_setopt(h_curl, CURLOPT_CUSTOMREQUEST, method.c_str());
  }

  return h_curl;
}

size_t RecvCB(void* buffer, size_t size, size_t nmemb, void* userp) {
  CurlBuffer* my_buffer = static_cast<CurlBuffer*>(userp);

  if (size * nmemb < 1) {
    return 0;
  }

  my_buffer->data = static_cast<char*>(buffer);

  return my_buffer->data.size();
}

}  // namespace

bool MakeAcquireRequest(const std::string& key_id, const std::string& secret,
                        const std::string& repo_path,
                        const std::string& repo_service_url,
                        CurlBuffer* buffer) {
  CURLcode ret = static_cast<CURLcode>(0);

  CURL* h_curl = PrepareCurl("POST");
  if (!h_curl) {
    return false;
  }

  const std::string payload = "{\"path\" : \"" + repo_path +
                              "\", \"api_version\" : \"" +
                              StringifyInt(gateway::APIVersion()) + "\"}";

  shash::Any hmac(shash::kSha1);
  shash::HmacString(secret, payload, &hmac);

  const std::string header_str = std::string("Authorization: ") + key_id + " " +
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
  if (ret) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "Make lease acquire request failed: %d. Reply: %s", ret,
             buffer->data.c_str());
  }

  curl_easy_cleanup(h_curl);
  h_curl = NULL;

  return !ret;
}

bool MakeEndRequest(const std::string& method, const std::string& key_id,
                    const std::string& secret, const std::string& session_token,
                    const std::string& repo_service_url,
                    const std::string& request_payload, CurlBuffer* reply) {
  CURLcode ret = static_cast<CURLcode>(0);

  CURL* h_curl = PrepareCurl(method);
  if (!h_curl) {
    return false;
  }

  shash::Any hmac(shash::kSha1);
  shash::HmacString(secret, session_token, &hmac);

  const std::string header_str = std::string("Authorization: ") + key_id + " " +
                                 Base64(hmac.ToString(false));
  struct curl_slist* auth_header = NULL;
  auth_header = curl_slist_append(auth_header, header_str.c_str());
  curl_easy_setopt(h_curl, CURLOPT_HTTPHEADER, auth_header);

  curl_easy_setopt(h_curl, CURLOPT_URL,
                   (repo_service_url + "/leases/" + session_token).c_str());
  if (request_payload != "") {
    curl_easy_setopt(h_curl, CURLOPT_POSTFIELDSIZE_LARGE,
                     static_cast<curl_off_t>(request_payload.length()));
    curl_easy_setopt(h_curl, CURLOPT_POSTFIELDS, request_payload.c_str());
  } else {
    curl_easy_setopt(h_curl, CURLOPT_POSTFIELDSIZE_LARGE,
                     static_cast<curl_off_t>(0));
    curl_easy_setopt(h_curl, CURLOPT_POSTFIELDS, NULL);
  }
  curl_easy_setopt(h_curl, CURLOPT_WRITEFUNCTION, RecvCB);
  curl_easy_setopt(h_curl, CURLOPT_WRITEDATA, reply);

  ret = curl_easy_perform(h_curl);
  if (ret) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "Lease end request - curl_easy_perform failed: %d", ret);
  }

  const bool ok = (reply->data == "{\"status\":\"ok\"}");
  if (!ok) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "Lease end request - error reply: %s",
             reply->data.c_str());
  }

  curl_easy_cleanup(h_curl);
  h_curl = NULL;

  return ok && !ret;
}
