/**
 * This file is part of the CernVM File System.
 */

#include "subscriber_ws.h"

#include <vector>

#include "atomic.h"
#include "logging.h"
#include "messages.h"
#include "url.h"
#include "util/pointer.h"
#include "util/string.h"

namespace {

const int kLogError = DefaultLogging::error;

const unsigned char kPingToken = 123;
const int kPingInterval = 50000000;  // 50 sec

const int kWsLogLevel = LLL_ERR | LLL_WARN | LLL_NOTICE | LLL_INFO | LLL_USER;

void LogWs(int level, const char* line) {
  int dest = kLogDebug;
  if (level & LLL_ERR) {
    dest = kLogError;
  }

  LogCvmfs(kLogCvmfs, dest, line);
}

struct Settings {
  std::string protocol;
  std::string host;
  std::string path;
  int port;
  std::string topic;
};

struct PerSessionStorage {
  std::string* message;
  bool send_ping;
};

struct ContextData {
  struct lws_context* context;
  struct lws_vhost* vhost;
  struct lws* client_wsi;

  notify::SubscriberWS* subscriber;

  bool* should_stop;

  Settings settings;
};

int ConnectClient(ContextData* cd) {
  struct lws_client_connect_info i;
  memset(&i, 0, sizeof(i));

  const std::string host =
      cd->settings.host + ":" + StringifyUint(cd->settings.port);

  i.context = cd->context;
  i.port = cd->settings.port;
  i.address = cd->settings.host.c_str();
  i.path = cd->settings.path.c_str();
  i.host = host.c_str();
  i.origin = host.c_str();
  i.ssl_connection = 0;
  i.vhost = cd->vhost;
  i.pwsi = &(cd->client_wsi);

  return !lws_client_connect_via_info(&i);
}

void ScheduleCallback(struct lws* wsi, int reason, int secs) {
  lws_timed_callback_vh_protocol(lws_get_vhost(wsi), lws_get_protocol(wsi),
                                 reason, secs);
}

}  // namespace

namespace notify {

SubscriberWS::SubscriberWS(const std::string& server_url)
    : server_url_(server_url) {}

SubscriberWS::~SubscriberWS() {}

bool SubscriberWS::Subscribe(const std::string& topic) {
  lws_set_log_level(kWsLogLevel, &LogWs);

  UniquePtr<Url> url(Url::Parse(server_url_));
  if (!url.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogError, "Could not parse notification server url",
             server_url_.c_str());
    return true;
  }

  Settings settings;
  settings.protocol = url->protocol();
  settings.host = url->host();
  settings.path = url->path();
  settings.port = url->port();
  settings.topic = topic;

  // C-style structs needed to initialize the libwebsockets session
  const struct lws_protocols protocols[] = {
      {"cvmfs", SubscriberWS::WSCallback, sizeof(PerSessionStorage), 1024, 0,
       NULL, 0},
      {NULL, NULL, 0, 0}};

  bool should_stop = false;

  const struct lws_protocol_vhost_options pvo_should_stop = {
      NULL, NULL, "should_stop", reinterpret_cast<char*>(&should_stop)};
  const struct lws_protocol_vhost_options pvo_subscriber = {
      &pvo_should_stop, NULL, "subscriber", reinterpret_cast<char*>(this)};
  const struct lws_protocol_vhost_options pvo_settings = {
      &pvo_subscriber, NULL, "settings", reinterpret_cast<char*>(&settings)};
  const struct lws_protocol_vhost_options pvo = {NULL, &pvo_settings, "cvmfs",
                                                 ""};

  struct lws_context_creation_info info;
  memset(&info, 0, sizeof info);
  info.port = CONTEXT_PORT_NO_LISTEN;
  info.protocols = protocols;
  info.pvo = &pvo;
  info.pt_serv_buf_size = 32 * 1024;
  info.options = LWS_SERVER_OPTION_VALIDATE_UTF8;

  struct lws_context* context = lws_create_context(&info);
  if (!context) {
    LogCvmfs(kLogCvmfs, kLogError, "Could not create libwebsocket context.");
    return false;
  }

  int err = 0;
  while (!err && !should_stop) {
    err = lws_service(context, 1000);
  }

  bool ret = true;
  if (err) {
    LogCvmfs(kLogCvmfs, kLogError,
             "SubscriberWS event loop finished with error: %d", err);
    ret = false;
  }

  lws_context_destroy(context);

  return ret;
}

int SubscriberWS::WSCallback(struct lws* wsi, enum lws_callback_reasons reason,
                             void* user, void* in, size_t len) {
  PerSessionStorage* pss = static_cast<PerSessionStorage*>(user);
  ContextData* cd = static_cast<ContextData*>(
      lws_protocol_vh_priv_get(lws_get_vhost(wsi), lws_get_protocol(wsi)));

  switch (reason) {
    case LWS_CALLBACK_PROTOCOL_INIT: {
      cd = static_cast<ContextData*>(lws_protocol_vh_priv_zalloc(
          lws_get_vhost(wsi), lws_get_protocol(wsi), sizeof(ContextData)));
      if (!cd) return -1;

      cd->context = lws_get_context(wsi);
      cd->vhost = lws_get_vhost(wsi);

      // Note: libwebsockets is a C library. Passing parameters into the
      //       connection context is done with a link list storing generic
      //       pointers (void*) to the parameter values. C-style casts back
      //       to the original value types are needed. See:
      //
      //   https://libwebsockets.org/git/libwebsockets/tree/minimal-examples
      //
      //       Using C++-style casts would not increase readability in this
      //       case, since each C-style cast would need to be replaced with
      //       a const_cast, followed by a reinterpret_cast.
      cd->subscriber =
          (SubscriberWS*)(lws_pvo_search(  // NOLINT(readability/casting)
                              (const struct lws_protocol_vhost_options*)in,
                              "subscriber")
                              ->value);

      cd->should_stop =
          (bool*)(lws_pvo_search(  // NOLINT(readability/casting)
            (const struct lws_protocol_vhost_options*) in, "should_stop")
                      ->value);

      cd->settings =
          *((Settings*)lws_pvo_search(  // NOLINT(readability/casting)
                (const struct lws_protocol_vhost_options*)in, "settings")
                ->value);
      if (ConnectClient(cd)) {
        ScheduleCallback(wsi, LWS_CALLBACK_USER, 1);
      }
      break;
    }
    case LWS_CALLBACK_CLIENT_ESTABLISHED: {
      pss->message = new std::string();
      pss->send_ping = false;
      lws_callback_on_writable(wsi);
      break;
    }
    case LWS_CALLBACK_CLIENT_WRITEABLE: {
      // Send a Websocket PING
      if (pss->send_ping) {
        unsigned char token = kPingToken;
        lws_write(wsi, &token, 1, LWS_WRITE_PING);
        lws_set_timer_usecs(wsi, kPingInterval);
        break;
      }

      // Send initial subscription request
      std::string buf(LWS_PRE, '0');
      buf += "{\"version\":" + StringifyInt(notify::msg::kProtocolVersion) +
             ",\"repository\":\"" + cd->settings.topic + "\"}";
      int body_size = buf.size() - LWS_PRE;
      int bytes_sent =
          lws_write(wsi, reinterpret_cast<unsigned char*>(&buf[LWS_PRE]),
                    body_size, LWS_WRITE_BINARY);
      if (bytes_sent != body_size) {
        LogCvmfs(kLogCvmfs, kLogError, "Could not send subscription request.");
        return -1;
      }
      lws_set_timer_usecs(wsi, kPingInterval);
      pss->send_ping = true;
      break;
    }
    case LWS_CALLBACK_CLIENT_RECEIVE: {
      // Only handling binary frames
      if (!lws_frame_is_binary(wsi)) {
        break;
      }

      if (lws_is_first_fragment(wsi)) {
        pss->message->resize(0);
      }

      size_t current_size = pss->message->size();
      pss->message->resize(current_size + len);
      memcpy(&(*pss->message)[current_size], in, len);

      if (lws_is_final_fragment(wsi)) {
        if (!cd->subscriber->Consume(cd->settings.topic, *(pss->message))) {
          *(cd->should_stop) = true;
          delete pss->message;
          pss->message = NULL;
          return 0;
        }
      }

      break;
    }
    case LWS_CALLBACK_CLIENT_CONNECTION_ERROR: {
      ScheduleCallback(wsi, LWS_CALLBACK_USER, 1);
      if (pss->message) {
        delete pss->message;
        pss->message = NULL;
      }
      lws_cancel_service(lws_get_context(wsi));
      break;
    }
    case LWS_CALLBACK_CLIENT_CLOSED: {
      if (pss->message) {
        delete pss->message;
        pss->message = NULL;
      }
      lws_cancel_service(lws_get_context(wsi));
      break;
    }
    case LWS_CALLBACK_TIMER: {
      lws_callback_on_writable(wsi);
      break;
    }
    case LWS_CALLBACK_USER: {
      if (ConnectClient(cd)) {
        ScheduleCallback(wsi, LWS_CALLBACK_USER, 1);
      }
      break;
    }
    default:
      // LogCvmfs(kLogCvmfs, kLogDebug,
      //          "Unhandled websocket event: %d", reason);
      break;
  }

  return 0;
}

}  // namespace notify
