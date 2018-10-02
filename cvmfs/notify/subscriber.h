/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NOTIFY_SUBSCRIBER_H_
#define CVMFS_NOTIFY_SUBSCRIBER_H_

#include <string>

namespace notify {

class Subscriber {
 public:
  Subscriber() {}
  virtual ~Subscriber() {}

  virtual bool Subscribe(const std::string& topic) = 0;

 protected:
  /**
   * Consume a message
   *
   * Consume the message and returns true or false, if the subscription
   * should continue or stop, respectively.
   */
  virtual bool Consume(const std::string& topic,
                       const std::string& msg_text) = 0;
};

}  // namespace notify

#endif  // CVMFS_NOTIFY_SUBSCRIBER_H_
