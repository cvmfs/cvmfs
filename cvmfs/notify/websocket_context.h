/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NOTIFY_WEBSOCKET_CONTEXT_H_
#define CVMFS_NOTIFY_WEBSOCKET_CONTEXT_H_

#include <string>

#include "libwebsockets.h"

#include "url.h"

namespace notify {

class SubscriberWS;
struct ConnectionData;

/**
 * Establishes and maintains a Websocket connection to the notification system
 *
 * The Websocket connection state-machine is implemented here. Includes basic
 * connection retry and keep-alive (WS ping/pong) support.
 */
class WebsocketContext {
 public:
  enum Status { kOk, kError };

  static WebsocketContext* Create(const Url& server_url,
                                  const std::string& topic,
                                  SubscriberWS* subscriber);

  ~WebsocketContext();

  Status Run();

 private:
  enum State {
    kNotConnected,
    kConnected,
    kSubscribed,
    kFinished,
  };

  WebsocketContext(const Url& server_url, const std::string& topic,
                   SubscriberWS* subscriber);

  void SetState(State new_state);

  bool Connect();

  void Finish(Status s);

  static int MainCallback(struct lws* wsi, enum lws_callback_reasons reason,
                          void* user, void* in, size_t len);

  static int NotConnectedCallback(ConnectionData** cd, struct lws* wsi,
                                  enum lws_callback_reasons reason, void* user,
                                  void* in, size_t len);
  static int ConnectedCallback(ConnectionData* cd, struct lws* wsi,
                               enum lws_callback_reasons reason, void* user,
                               void* in, size_t len);
  static int SubscribedCallback(ConnectionData* cd, struct lws* wsi,
                                enum lws_callback_reasons reason, void* user,
                                void* in, size_t len);
  static int FinishedCallback(ConnectionData* cd, struct lws* wsi,
                              enum lws_callback_reasons reason, void* user,
                              void* in, size_t len);

  std::string message_;

  State state_;

  // Status at the end of the connnection
  Status status_;

  std::string host_;
  std::string path_;
  int port_;
  std::string host_port_str_;
  std::string topic_;
  SubscriberWS* subscriber_;

  struct lws_vhost* vhost_;
  struct lws* wsi_;
  struct lws_context* lws_ctx_;
};

}  // namespace notify

#endif  // CVMFS_NOTIFY_WEBSOCKET_CONTEXT_H_
