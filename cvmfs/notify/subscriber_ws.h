
/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NOTIFY_SUBSCRIBER_WS_H_
#define CVMFS_NOTIFY_SUBSCRIBER_WS_H_

#include "subscriber.h"

#include <string>

namespace notify {

class SubscriberWS : public Subscriber {
  /**
   * The WebsocketContext class needs to call the Subscriber::Consume
   * protected method during its event loop
   */
  friend class WebsocketContext;

 public:
  explicit SubscriberWS(const std::string& server_url);
  virtual ~SubscriberWS();

  virtual bool Subscribe(const std::string& topic);

 private:
  std::string server_url_;
};

}  // namespace notify

#endif  // CVMFS_NOTIFY_SUBSCRIBER_WS_H_
