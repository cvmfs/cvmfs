/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NOTIFY_PUBLISHER_H_
#define CVMFS_NOTIFY_PUBLISHER_H_

#include <string>

namespace notify {

class Publisher {
 public:
  virtual ~Publisher();

  virtual bool Init();
  virtual bool Publish(const std::string& msg, const std::string& topic) = 0;
  virtual bool Finalize();
};

}  // namespace notify

#endif  // CVMFS_NOTIFY_PUBLISHER_H_
