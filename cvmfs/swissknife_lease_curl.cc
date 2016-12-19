/**
 * This file is part of the CernVM File System.
 */

#include "swissknife_lease_curl.h"

#include <iostream>

size_t RecvCB(void *buffer, size_t size, size_t nmemb, void *userp) {
  CurlBuffer* my_buffer = (CurlBuffer*)userp;

  if (size * nmemb < 1) {
    return 0;
  }

  my_buffer->data = static_cast<char*>(buffer);

  return my_buffer->data.size();
}

CURL* PrepareCurl(const char* method) {
  CURL* h_curl = curl_easy_init();

  if (h_curl) {
    curl_easy_setopt(h_curl, CURLOPT_NOPROGRESS, 1L);
    curl_easy_setopt(h_curl, CURLOPT_USERAGENT, "curl/7.47.0");
    curl_easy_setopt(h_curl, CURLOPT_MAXREDIRS, 50L);
    curl_easy_setopt(h_curl, CURLOPT_CUSTOMREQUEST, method);
    curl_easy_setopt(h_curl, CURLOPT_TCP_KEEPALIVE, 1L);
  }

  return h_curl;
}

bool MakeAcquireRequest(const std::string& user_name,
                        const std::string& lease_fqdn,
                        const std::string& repo_service_url,
                        CurlBuffer& buffer) {
  CURLcode ret = static_cast<CURLcode>(0);

  CURL* h_curl = PrepareCurl("POST");
  if (h_curl) {
    // Prepare payload
    std::string payload = "{\"user\" : \"" + user_name + "\", \"path\" : \"" + lease_fqdn + "\"}";

    // Make request to acquire lease from repo services
    curl_easy_setopt(h_curl, CURLOPT_URL, (repo_service_url + "/api/leases").c_str());
    curl_easy_setopt(h_curl, CURLOPT_POSTFIELDSIZE_LARGE, static_cast<curl_off_t>(payload.length()));
    curl_easy_setopt(h_curl, CURLOPT_POSTFIELDS, payload.c_str());
    curl_easy_setopt(h_curl, CURLOPT_WRITEFUNCTION, RecvCB);
    curl_easy_setopt(h_curl, CURLOPT_WRITEDATA, &buffer);

    ret = curl_easy_perform(h_curl);

    curl_easy_cleanup(h_curl);
    h_curl = NULL;

    return ! ret;
  }

  return false;
}

bool MakeDeleteRequest(const std::string& session_token,
                       const std::string& repo_service_url,
                       CurlBuffer& buffer) {
  CURLcode ret = static_cast<CURLcode>(0);

  CURL* h_curl = PrepareCurl("DELETE");
  if (h_curl) {

    curl_easy_setopt(h_curl, CURLOPT_URL, (repo_service_url + "/api/leases/" + session_token).c_str());
    curl_easy_setopt(h_curl, CURLOPT_POSTFIELDSIZE_LARGE, static_cast<curl_off_t>(0));
    curl_easy_setopt(h_curl, CURLOPT_POSTFIELDS, 0);
    curl_easy_setopt(h_curl, CURLOPT_WRITEFUNCTION, RecvCB);
    curl_easy_setopt(h_curl, CURLOPT_WRITEDATA, &buffer);

    ret = curl_easy_perform(h_curl);

    curl_easy_cleanup(h_curl);
    h_curl = NULL;

    return ! ret;
  }

  return false;
}


