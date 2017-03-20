/**
 * This file is part of the CernVM File System.
 */

#include "swissknife_lease_curl.h"

#include "cvmfs_config.h"

#include "hash.h"
#include "logging.h"
#include "util/string.h"

size_t RecvCB(void* buffer, size_t size, size_t nmemb, void* userp) {
  CurlBuffer* my_buffer = static_cast<CurlBuffer*>(userp);

  if (size * nmemb < 1) {
    return 0;
  }

  my_buffer->data = static_cast<char*>(buffer);

  return my_buffer->data.size();
}

CURL* PrepareCurl(const char* method) {
  const char* user_agent_string = "cvmfs/" VERSION;

  CURL* h_curl = curl_easy_init();

  if (h_curl) {
    curl_easy_setopt(h_curl, CURLOPT_NOPROGRESS, 1L);
    curl_easy_setopt(h_curl, CURLOPT_USERAGENT, user_agent_string);
    curl_easy_setopt(h_curl, CURLOPT_MAXREDIRS, 50L);
    curl_easy_setopt(h_curl, CURLOPT_CUSTOMREQUEST, method);
    curl_easy_setopt(h_curl, CURLOPT_TCP_KEEPALIVE, 1L);
  }

  return h_curl;
}

bool MakeAcquireRequest(const std::string& key_file,
                        const std::string& repo_path,
                        const std::string& repo_service_url,
                        CurlBuffer* buffer) {
  CURLcode ret = static_cast<CURLcode>(0);

  // Prepare payload
  FILE* key_file_fd = std::fopen(key_file.c_str(), "r");
  if (!key_file_fd) {
    return false;
  }

  std::string key_id;
  std::string secret;
  GetLineFile(key_file_fd, &key_id);
  GetLineFile(key_file_fd, &secret);
  fclose(key_file_fd);

  CURL* h_curl = PrepareCurl("POST");
  if (!h_curl) {
    return false;
  }

  const std::string payload = "{\"path\" : \"" + repo_path + "\"}";

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

  curl_easy_cleanup(h_curl);
  h_curl = NULL;

  return !ret;
}

bool MakeDeleteRequest(const std::string& key_file,
                       const std::string& session_token,
                       const std::string& repo_service_url,
                       CurlBuffer* buffer) {
  CURLcode ret = static_cast<CURLcode>(0);

  FILE* key_file_fd = std::fopen(key_file.c_str(), "r");
  if (!key_file_fd) {
    return false;
  }

  std::string key_id;
  std::string secret;
  GetLineFile(key_file_fd, &key_id);
  GetLineFile(key_file_fd, &secret);
  fclose(key_file_fd);

  CURL* h_curl = PrepareCurl("DELETE");
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
  curl_easy_setopt(h_curl, CURLOPT_POSTFIELDSIZE_LARGE,
                   static_cast<curl_off_t>(0));
  curl_easy_setopt(h_curl, CURLOPT_POSTFIELDS, 0);
  curl_easy_setopt(h_curl, CURLOPT_WRITEFUNCTION, RecvCB);
  curl_easy_setopt(h_curl, CURLOPT_WRITEDATA, buffer);

  ret = curl_easy_perform(h_curl);

  curl_easy_cleanup(h_curl);
  h_curl = NULL;

  return !ret;
}
