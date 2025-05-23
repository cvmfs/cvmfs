/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NOTIFY_SUBSCRIBER_SUPERVISOR_H_
#define CVMFS_NOTIFY_SUBSCRIBER_SUPERVISOR_H_

#include <string>

#include "supervisor.h"

namespace notify {

class Subscriber;

/**
 * Supervise the subscription to the notification system backend
 */
class SubscriberSupervisor : public Supervisor {
 public:
  SubscriberSupervisor(notify::Subscriber *s, std::string t, int max_retries,
                       uint64_t interval);
  virtual ~SubscriberSupervisor();

  virtual bool Task();

 private:
  notify::Subscriber *subscriber_;
  std::string topic_;
};

}  // namespace notify

#endif  // CVMFS_NOTIFY_SUBSCRIBER_SUPERVISOR_H_
