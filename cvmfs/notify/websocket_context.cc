/**
 * This file is part of the CernVM File System.
 */

#include "websocket_context.h"

#include <cassert>

#include "logging.h"
#include "messages.h"
#include "subscriber_ws.h"

#include "util/pointer.h"
#include "util/string.h"

namespace {

const LogFacilities& kLogInfo = DefaultLogging::info;
const LogFacilities& kLogError = DefaultLogging::error;

const unsigned char kPingToken = 123;
const int kPingInterval = 5000000;  // 50 sec

const int kWsLogLevel = LLL_ERR | LLL_WARN | LLL_NOTICE | LLL_INFO | LLL_USER;

void LogWs(int level, const char* line) {
  const LogFacilities& dest = (level & LLL_ERR) ? kLogError : kLogInfo;
  LogCvmfs(kLogCvmfs, dest, line);
}

void ScheduleCallback(struct lws* wsi, int reason, int secs) {
  lws_timed_callback_vh_protocol(lws_get_vhost(wsi), lws_get_protocol(wsi),
                                 reason, secs);
}

int WSWrite(struct lws* wsi, const std::string& msg,
            enum lws_write_protocol protocol) {
  std::string buf(LWS_PRE, '0');
  buf += msg;
  return lws_write(wsi, reinterpret_cast<unsigned char*>(&buf[LWS_PRE]),
                   msg.size(), protocol);
}

struct SessionData {};

}  // namespace

namespace notify {

struct ConnectionData {
  notify::WebsocketContext* ctx;
};

WebsocketContext* WebsocketContext::Create(const Url& server_url,
                                           const std::string& topic,
                                           SubscriberWS* subscriber) {
  UniquePtr<WebsocketContext> ctx(
      new WebsocketContext(server_url, topic, subscriber));
  if (!ctx.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogError,
             "WebsocketContext - could not create object.");
    return NULL;
  }

  return ctx.Release();
}

WebsocketContext::Status WebsocketContext::Run() {
  lws_set_log_level(kWsLogLevel, &LogWs);

  // C-style structs needed to initialize the libwebsockets session
  struct lws_protocols protocols[] = {
      {"cvmfs", MainCallback, sizeof(SessionData), 1024, 0, NULL, 0},
      {NULL, NULL, 0, 0}};

  struct lws_protocol_vhost_options pvo_context = {
      NULL, NULL, "context", reinterpret_cast<char*>(this)};
  struct lws_protocol_vhost_options pvo = {NULL, &pvo_context, "cvmfs", ""};

  struct lws_context_creation_info info;
  memset(&info, 0, sizeof(info));
  info.port = CONTEXT_PORT_NO_LISTEN;
  info.protocols = protocols;
  info.pvo = &pvo;
  info.pt_serv_buf_size = 32 * 1024;
  info.options = LWS_SERVER_OPTION_VALIDATE_UTF8;

  lws_ctx_ = lws_create_context(&info);
  if (!lws_ctx_) {
    LogCvmfs(kLogCvmfs, kLogError,
             "WebsocketContext - could not create libwebsocket context.");
    return kError;
  }

  assert(state_ == kNotConnected);
  int err = 0;
  while (!err && (state_ != kFinished)) {
    err = lws_service(lws_ctx_, 1000);
  }
  lws_context_destroy(lws_ctx_);

  return err ? kError : kOk;
}

WebsocketContext::WebsocketContext(const Url& server_url,
                                   const std::string& topic,
                                   SubscriberWS* subscriber)
    : message_(),
      state_(kNotConnected),
      host_(server_url.host()),
      path_(server_url.path()),
      port_(server_url.port()),
      host_port_str_(host_ + ":" + StringifyUint(port_)),
      topic_(topic),
      subscriber_(subscriber),
      vhost_(NULL),
      wsi_(NULL),
      lws_ctx_(NULL) {
  SetState(kNotConnected);
}

WebsocketContext::~WebsocketContext() {}

void WebsocketContext::SetState(State new_state) {
  LogCvmfs(kLogCvmfs, kLogInfo, "WebsocketContext - entered state: %d",
           new_state);
  state_ = new_state;
}

bool WebsocketContext::Connect() {
  struct lws_client_connect_info i;
  memset(&i, 0, sizeof(i));

  i.context = lws_ctx_;
  i.port = port_;
  i.address = host_.c_str();
  i.path = path_.c_str();
  i.host = host_port_str_.c_str();
  i.origin = host_port_str_.c_str();
  i.ssl_connection = 0;
  i.vhost = vhost_;
  i.pwsi = &(wsi_);

  return lws_client_connect_via_info(&i) != NULL;
}

void WebsocketContext::Finish() { SetState(kFinished); }

int WebsocketContext::MainCallback(struct lws* wsi,
                                   enum lws_callback_reasons reason, void* user,
                                   void* in, size_t len) {
  ConnectionData* cd = static_cast<ConnectionData*>(
      lws_protocol_vh_priv_get(lws_get_vhost(wsi), lws_get_protocol(wsi)));

  // Establishing a connection needs to be treated separately.
  // ConnectionData isn't allocated at this point
  switch (reason) {
    case LWS_CALLBACK_PROTOCOL_INIT:
    case LWS_CALLBACK_CLIENT_ESTABLISHED:
    case LWS_CALLBACK_USER:
      return NotConnectedCallback(&cd, wsi, reason, user, in, len);
    case LWS_CALLBACK_GET_THREAD_ID:
      return 0;
    default:
      LogCvmfs(kLogCvmfs, kLogInfo, "WebsocketContext - reason: %d", reason);
      //return 0;
  }

  switch (cd->ctx->state_) {
    case kConnected:
      return ConnectedCallback(cd, wsi, reason, user, in, len);
    case kSubscribed:
      return SubscribedCallback(cd, wsi, reason, user, in, len);
    case kFinished:
      return FinishedCallback(cd, wsi, reason, user, in, len);
    default:
      return 0;
  }
}

int WebsocketContext::NotConnectedCallback(ConnectionData** cd, struct lws* wsi,
                                           enum lws_callback_reasons reason,
                                           void* user, void* in, size_t len) {
  switch (reason) {
    case LWS_CALLBACK_PROTOCOL_INIT: {
      LogCvmfs(kLogCvmfs, kLogInfo,
               "WebsocketContext - LWS_CALLBACK_PROTOCOL_INIT State");
      *cd = static_cast<ConnectionData*>(lws_protocol_vh_priv_zalloc(
          lws_get_vhost(wsi), lws_get_protocol(wsi), sizeof(ConnectionData)));
      if (!*cd) {
        return -1;
      }

      (*cd)->ctx =
          (WebsocketContext*)(lws_pvo_search(  // NOLINT(readability/casting)
                                  (const struct lws_protocol_vhost_options*)in,
                                  "context")
                                  ->value);

      (*cd)->ctx->vhost_ = lws_get_vhost(wsi);

      if (!(*cd)->ctx->Connect()) {
        ScheduleCallback(wsi, LWS_CALLBACK_USER, 1);
      }
      break;
    }
    case LWS_CALLBACK_CLIENT_ESTABLISHED: {
      LogCvmfs(kLogCvmfs, kLogInfo,
               "WebsocketContext - LWS_CALLBACK_CLIENT_ESTABLISHED");
      (*cd)->ctx->SetState(kConnected);
      lws_callback_on_writable(wsi);
      break;
    }
    case LWS_CALLBACK_USER: {
      LogCvmfs(kLogCvmfs, kLogInfo, "WebsocketContext - LWS_CALLBACK_USER");
      if (!(*cd)->ctx->Connect()) {
        ScheduleCallback(wsi, LWS_CALLBACK_USER, 1);
      }
      break;
    }
    default:
      break;
  }
  return 0;
}

int WebsocketContext::ConnectedCallback(ConnectionData* cd, struct lws* wsi,
                                        enum lws_callback_reasons reason,
                                        void* user, void* in, size_t len) {
  LogCvmfs(kLogCvmfs, kLogInfo, "WebsocketContext::ConnectedCallback");
  switch (reason) {
    case LWS_CALLBACK_CLIENT_WRITEABLE: {
      LogCvmfs(kLogCvmfs, kLogInfo,
               "WebsocketContext - LWS_CALLBACK_CLIENT_WRITABLE");
      // Send initial subscription request
      std::string msg =
          "{\"version\":" + StringifyInt(notify::msg::kProtocolVersion) +
          ",\"repository\":\"" + cd->ctx->topic_ + "\"}";
      int bytes_sent = WSWrite(wsi, msg, LWS_WRITE_BINARY);
      if (bytes_sent == -1) {
        LogCvmfs(kLogCvmfs, kLogError,
                 "WebsocketContext - could not send subscription request.");
        cd->ctx->Finish();
        return -1;
      }
      if (static_cast<size_t>(bytes_sent) < msg.size()) {
        LogCvmfs(kLogCvmfs, kLogInfo,
                 "WebsocketContext - incomplete send: %d / %d.", bytes_sent,
                 msg.size());
        break;
      }
      lws_set_timer_usecs(wsi, kPingInterval);
      cd->ctx->SetState(kSubscribed);
      break;
    }
    default:
      break;
  }
  return 0;
}

int WebsocketContext::SubscribedCallback(ConnectionData* cd, struct lws* wsi,
                                         enum lws_callback_reasons reason,
                                         void* user, void* in, size_t len) {
  LogCvmfs(kLogCvmfs, kLogInfo, "WebsocketContext::SubscribedCallback");
  switch (reason) {
    case LWS_CALLBACK_CLIENT_WRITEABLE: {
      LogCvmfs(kLogCvmfs, kLogInfo,
               "WebsocketContext - LWS_CALLBACK_CLIENT_WRITABLE");
      // Send a Websocket PING
      unsigned char token = kPingToken;
      int bytes_sent = lws_write(wsi, &token, sizeof(token), LWS_WRITE_PING);
      if (bytes_sent == -1) {
        LogCvmfs(kLogCvmfs, kLogError,
                 "WebsocketContext - could not send ping: %d", bytes_sent);
        cd->ctx->Finish();
        return -1;
      }
      lws_set_timer_usecs(wsi, kPingInterval);
      break;
    }
    case LWS_CALLBACK_CLIENT_RECEIVE: {
      LogCvmfs(kLogCvmfs, kLogInfo,
               "WebsocketContext - LWS_CALLBACK_CLIENT_RECEIVE");

      if (lws_is_first_fragment(wsi)) {
        cd->ctx->message_.resize(0);
      }

      size_t current_size = cd->ctx->message_.size();
      cd->ctx->message_.resize(current_size + len);
      memcpy(&(cd->ctx->message_[current_size]), in, len);

      if (lws_is_final_fragment(wsi)) {
        if (!cd->ctx->subscriber_->Consume(cd->ctx->topic_,
                                           cd->ctx->message_)) {
          cd->ctx->Finish();
          return -1;
        }
      }
      break;
    }
    case LWS_CALLBACK_TIMER: {
      LogCvmfs(kLogCvmfs, kLogInfo, "WebsocketContext - LWS_CALLBACK_TIMER");
      lws_callback_on_writable(wsi);
      break;
    }
    default:
      break;
  }

  return 0;
}

int WebsocketContext::FinishedCallback(ConnectionData* cd, struct lws* wsi,
                                       enum lws_callback_reasons reason,
                                       void* user, void* in, size_t len) {
  LogCvmfs(kLogCvmfs, kLogInfo, "WebsocketContext::FinishedCallback");
  return 0;
}

}  // namespace notify
