/**
 * This file is part of the CernVM File System.
 */

#include "subscriber_ws.h"

#include <vector>

#include "logging.h"
#include "url.h"
#include "util/pointer.h"
#include "websocket_context.h"

namespace {

const LogFacilities& kLogInfo = DefaultLogging::info;
const LogFacilities& kLogError = DefaultLogging::error;

}  // namespace

namespace notify {

SubscriberWS::SubscriberWS(const std::string& server_url)
    : server_url_(server_url) {}

SubscriberWS::~SubscriberWS() {}

bool SubscriberWS::Subscribe(const std::string& topic) {
  UniquePtr<Url> url(Url::Parse(server_url_));
  if (!url.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogError,
             "SubscriberWS - could not parse notification server url",
             server_url_.c_str());
    return false;
  }

  UniquePtr<WebsocketContext> ctx(WebsocketContext::Create(*url, topic, this));
  if (!ctx.IsValid()) {
    LogCvmfs(
        kLogCvmfs, kLogError,
        "SubscriberWS - could not initialize websocket connection context.");
    return false;
  }

  WebsocketContext::Status status = ctx->Run();
  if (status != WebsocketContext::kOk) {
    LogCvmfs(kLogCvmfs, kLogError,
             "SubscriberWS - event loop finished witherror: %d", status);
    return false;
  }

  return true;
}

}  // namespace notify
