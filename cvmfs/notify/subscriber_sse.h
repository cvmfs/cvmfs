
/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NOTIFY_SUBSCRIBER_SSE_H_
#define CVMFS_NOTIFY_SUBSCRIBER_SSE_H_

#include "subscriber.h"

#include <string>

namespace notify {

/**
 * Notification system subscriber based on server-sent events (SSE)
 */
class SubscriberSSE : public Subscriber {
 public:
  explicit SubscriberSSE(const std::string& server_url);
  virtual ~SubscriberSSE();

  virtual bool Subscribe(const std::string& topic);

 private:
  std::string server_url_;
};

}  // namespace notify

#endif  // CVMFS_NOTIFY_SUBSCRIBER_SSE_H_
