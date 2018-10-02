/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NOTIFY_PUBLISHER_HTTP_H_
#define CVMFS_NOTIFY_PUBLISHER_HTTP_H_

#include "publisher.h"

#include <string>

namespace notify {

class PublisherHTTP : public Publisher {
 public:
  explicit PublisherHTTP(const std::string& server_url);
  virtual ~PublisherHTTP();

  virtual bool Publish(const std::string& msg, const std::string& topic);

 private:
  std::string server_url_;

  bool initialized_;
};

}  // namespace notify

#endif  // CVMFS_NOTIFY_PUBLISHER_HTTP_H_
