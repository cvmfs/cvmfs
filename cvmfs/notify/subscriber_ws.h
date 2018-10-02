
/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NOTIFY_SUBSCRIBER_WS_H_
#define CVMFS_NOTIFY_SUBSCRIBER_WS_H_

#include "subscriber.h"

#include "libwebsockets.h"

namespace notify {

class SubscriberWS : public Subscriber {
 public:
  SubscriberWS(const std::string& server_url);
  virtual ~SubscriberWS();

  virtual bool Subscribe(const std::string& topic);

 private:
  static int WSCallback(struct lws* wsi, enum lws_callback_reasons reason,
                        void* user, void* in, size_t len);

  std::string server_url_;
};

}  // namespace notify

#endif  // CVMFS_NOTIFY_SUBSCRIBER_WS_H_
