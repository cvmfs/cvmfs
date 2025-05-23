/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NOTIFY_PUBLISHER_H_
#define CVMFS_NOTIFY_PUBLISHER_H_

#include <string>

namespace notify {

/**
 * Base class for publishing to the notification system
 */
class Publisher {
 public:
  virtual ~Publisher();

  /**
   * Perform (optional) initialization tasks before publishing
   */
  virtual bool Init();

  /**
   * Publish a message with the specified topic
   *
   * The message body and the message topic are given as strings
   */
  virtual bool Publish(const std::string &msg, const std::string &topic) = 0;

  /**
   * Perform (optional) finalization tasks after publishing the message
   */
  virtual bool Finalize();
};

}  // namespace notify

#endif  // CVMFS_NOTIFY_PUBLISHER_H_
