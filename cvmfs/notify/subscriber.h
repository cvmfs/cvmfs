/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NOTIFY_SUBSCRIBER_H_
#define CVMFS_NOTIFY_SUBSCRIBER_H_

#include <string>

namespace notify {

/**
 * Base class for creating a subscription to the notification system
 */
class Subscriber {
 public:
  enum Status {
    kContinue,
    kFinish,
    kError,
  };

  Subscriber() { }
  virtual ~Subscriber() { }

  /**
   * Subscribe to a specific message topic
   *
   * The only topics currently used by the notification system are repository
   * names. The subscription delivers activity messages containing the current
   * repository manifest. Returns false if an error occurred.
   */
  virtual bool Subscribe(const std::string &topic) = 0;

  /**
   * Unsubscribe
   *
   * Cancel an active subscription
   */
  virtual void Unsubscribe() { }

 protected:
  /**
   * Consume a message
   *
   * Consume the message and return the status value that can be used
   * to exit the subscription loop.
   */
  virtual Status Consume(const std::string &topic,
                         const std::string &msg_text) = 0;
};

}  // namespace notify

#endif  // CVMFS_NOTIFY_SUBSCRIBER_H_
