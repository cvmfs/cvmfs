/**
 * This file is part of the CernVM File System.
 */

#include "subscriber_sse.h"

#include <vector>

#include "duplex_curl.h"

#include "cvmfs_config.h"

#include "logging.h"
#include "url.h"
#include "util/pointer.h"

namespace {

const LogFacilities& kLogInfo = DefaultLogging::info;
const LogFacilities& kLogError = DefaultLogging::error;

}  // namespace

namespace notify {

SubscriberSSE::SubscriberSSE(const std::string& server_url)
    : server_url_(server_url) {}

SubscriberSSE::~SubscriberSSE() {}

bool SubscriberSSE::Subscribe(const std::string& topic) {
  UniquePtr<Url> url(Url::Parse(server_url_));

  if (!url.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogError,
             "SubscriberSSE - could not parse notification server url",
             server_url_.c_str());
    return false;
  }

  const char* user_agent_string = "cvmfs/" VERSION;

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
    LogCvmfs(kLogCvmfs, kLogError, "SubscriberSSE - event loop finished with error: %d. Reply: %s", ret,
             buffer.data.c_str());
  }

  curl_easy_cleanup(h_curl);
  h_curl = NULL;

  return !ret;
}

}  // namespace notify
