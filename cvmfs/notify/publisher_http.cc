/**
 * This file is part of the CernVM File System.
 */

#include "publisher_http.h"

#include "duplex_curl.h"



#include "util/logging.h"
#include "util/string.h"

namespace {

const LogFacilities& kLogError = DefaultLogging::error;

struct CurlBuffer {
  std::string data;
};

size_t RecvCB(void* buffer, size_t size, size_t nmemb, void* userp) {
  CurlBuffer* my_buffer = static_cast<CurlBuffer*>(userp);

  if (size * nmemb < 1) {
    return 0;
  }

  my_buffer->data = static_cast<char*>(buffer);

  return my_buffer->data.size();
}

}  // namespace

namespace notify {

PublisherHTTP::PublisherHTTP(const std::string& server_url)
    : server_url_(server_url + "/notifications/publish") {}

PublisherHTTP::~PublisherHTTP() {}

bool PublisherHTTP::Publish(const std::string& msg, const std::string& topic) {
  const char* user_agent_string = "cvmfs/" CVMFS_VERSION;

  CURL* h_curl = curl_easy_init();

  if (h_curl) {
    curl_easy_setopt(h_curl, CURLOPT_NOPROGRESS, 1L);
    curl_easy_setopt(h_curl, CURLOPT_USERAGENT, user_agent_string);
    curl_easy_setopt(h_curl, CURLOPT_MAXREDIRS, 50L);
    curl_easy_setopt(h_curl, CURLOPT_CUSTOMREQUEST, "POST");
  }

  if (!h_curl) {
    LogCvmfs(kLogCvmfs, kLogError, "Error initializing CURL context.");
    return false;
  }

  CurlBuffer buffer;
  // Make request to acquire lease from repo services
  curl_easy_setopt(h_curl, CURLOPT_URL, server_url_.c_str());
  curl_easy_setopt(h_curl, CURLOPT_POSTFIELDSIZE_LARGE,
                   static_cast<curl_off_t>(msg.length()));
  curl_easy_setopt(h_curl, CURLOPT_POSTFIELDS, msg.c_str());
  curl_easy_setopt(h_curl, CURLOPT_WRITEFUNCTION, RecvCB);
  curl_easy_setopt(h_curl, CURLOPT_WRITEDATA, &buffer);

  CURLcode ret = curl_easy_perform(h_curl);
  if (ret) {
    LogCvmfs(kLogCvmfs, kLogError, "POST request failed: %d. Reply: %s", ret,
             buffer.data.c_str());
  }

  curl_easy_cleanup(h_curl);
  h_curl = NULL;

  return !ret;
}

}  // namespace notify
