/**
 * This file is part of the CernVM File System.
 */

#include "subscriber_sse.h"



#include <vector>

#include "url.h"
#include "util/logging.h"
#include "util/pointer.h"
#include "util/string.h"

namespace {

const LogFacilities& kLogInfo = DefaultLogging::info;
const LogFacilities& kLogError = DefaultLogging::error;

}  // namespace

namespace notify {

SubscriberSSE::SubscriberSSE(const std::string& server_url)
    : Subscriber(),
      server_url_(server_url + "/notifications/subscribe"),
      topic_(),
      buffer_(),
      should_quit_(false) {}

SubscriberSSE::~SubscriberSSE() {}

bool SubscriberSSE::Subscribe(const std::string& topic) {
  UniquePtr<Url> url(Url::Parse(server_url_));

  if (!url.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogError,
             "SubscriberSSE - could not parse notification server url: %s\n",
             server_url_.c_str());
    return false;
  }

  this->topic_ = topic;

  std::string request = "{\"version\":1,\"repository\":\"" + topic + "\"}";

  const char* user_agent_string = "cvmfs/" CVMFS_VERSION;

  CURL* h_curl = curl_easy_init();
  if (h_curl == NULL) {
    LogCvmfs(kLogCvmfs, kLogError, "Could not create Curl handle\n");
    return false;
  }

  if (h_curl) {
    curl_easy_setopt(h_curl, CURLOPT_NOPROGRESS, 0L);
    curl_easy_setopt(h_curl, CURLOPT_USERAGENT, user_agent_string);
    curl_easy_setopt(h_curl, CURLOPT_MAXREDIRS, 50L);
    curl_easy_setopt(h_curl, CURLOPT_CUSTOMREQUEST, "GET");
  }

  if (!h_curl) {
    LogCvmfs(kLogCvmfs, kLogError,
             "SubscriberSSE - error initializing CURL context\n");
    return false;
  }

  // Make request to acquire lease from repo services
  curl_easy_setopt(h_curl, CURLOPT_URL, server_url_.c_str());
  curl_easy_setopt(h_curl, CURLOPT_POSTFIELDSIZE_LARGE,
                   static_cast<curl_off_t>(request.length()));
  curl_easy_setopt(h_curl, CURLOPT_POSTFIELDS, request.c_str());
  curl_easy_setopt(h_curl, CURLOPT_WRITEFUNCTION, CurlRecvCB);
  curl_easy_setopt(h_curl, CURLOPT_WRITEDATA, this);
  curl_easy_setopt(h_curl, CURLOPT_XFERINFOFUNCTION, CurlProgressCB);
  curl_easy_setopt(h_curl, CURLOPT_XFERINFODATA, this);

  bool success = true;
  CURLcode ret = curl_easy_perform(h_curl);
  if (ret && ret != CURLE_ABORTED_BY_CALLBACK) {
    LogCvmfs(kLogCvmfs, kLogError,
             "SubscriberSSE - event loop finished with error: %d. Reply: %s\n",
             ret, this->buffer_.c_str());
    success = false;
  }

  curl_easy_cleanup(h_curl);
  h_curl = NULL;

  return success;
}

void SubscriberSSE::Unsubscribe() {
  atomic_write32(&should_quit_, 1);
}

bool SubscriberSSE::ShouldQuit() const {
  return atomic_read32(&should_quit_);
}

void SubscriberSSE::AppendToBuffer(const std::string& s) {
  size_t start = 0;
  if (s.substr(0, 6) == "data: ") {
    start = 6;
  }
  buffer_ += s.substr(start);
}

void SubscriberSSE::ClearBuffer() { buffer_.clear(); }

size_t SubscriberSSE::CurlRecvCB(void* buffer, size_t size, size_t nmemb,
                                 void* userp) {
  notify::SubscriberSSE* sub = static_cast<notify::SubscriberSSE*>(userp);

  if (size * nmemb < 1) {
    return 0;
  }

  std::string buf(static_cast<char*>(buffer));

  std::vector<std::string> lines = SplitString(buf, '\n');

  if (lines.size() == 1) {
    sub->AppendToBuffer(lines[0]);
  } else {
    sub->AppendToBuffer(lines[0]);
    notify::Subscriber::Status st = sub->Consume(sub->topic_, sub->buffer_);
    sub->ClearBuffer();
    for (size_t i = 1; i < lines.size(); ++i) {
      if (lines[i].substr(0, 5) == "data: ") {
        sub->AppendToBuffer(lines[i]);
      }
    }
    switch (st) {
      case notify::Subscriber::kFinish:
        sub->Unsubscribe();
        break;
      case notify::Subscriber::kError:
        return 0;
      default:
        break;
    }
  }

  return size * nmemb;
}

int SubscriberSSE::CurlProgressCB(void* clientp, curl_off_t dltotal,
                                  curl_off_t dlnow, curl_off_t ultotal,
                                  curl_off_t ulnow) {
  notify::SubscriberSSE* sub = static_cast<notify::SubscriberSSE*>(clientp);
  if (sub->ShouldQuit()) {
    LogCvmfs(kLogCvmfs, kLogInfo,
             "SubscriberSSE - quit request received. Stopping\n");
    return 1;
  }

  return 0;
}

}  // namespace notify
