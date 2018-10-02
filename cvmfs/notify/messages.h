/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NOTIFY_MESSAGES_H_
#define CVMFS_NOTIFY_MESSAGES_H_

#include <string>

namespace notify {

namespace msg {

enum Version { kProtocolVersion = 1 };

class Message {
 public:
  virtual void ToJSONString(std::string* s) = 0;
  virtual bool FromJSONString(const std::string& s) = 0;
};

class Activity : public Message {
 public:
  Activity();
  virtual ~Activity();

  virtual void ToJSONString(std::string* s);
  virtual bool FromJSONString(const std::string& s);

  int version_;
  std::string timestamp_;
  std::string repository_;
  std::string manifest_;
};

}  // namespace msg

}  // namespace notify

#endif  // CVMFS_NOTIFY_MESSAGES_H_
