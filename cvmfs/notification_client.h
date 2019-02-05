/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NOTIFICATION_CLIENT_H_
#define CVMFS_NOTIFICATION_CLIENT_H_

#include <string>

#include "fuse_remount.h"
#include "notify/subscriber.h"

namespace signature {
  class SignatureManager;
}

class NotificationClient {
 public:
  NotificationClient(const std::string& config,
                     const std::string& repo_name,
                     FuseRemounter* remounter,
                     signature::SignatureManager* sig_mgr);
  virtual ~NotificationClient();

  void Spawn();

 private:
  static void* Run(void* instance);

  std::string config_;
  std::string repo_name_;
  FuseRemounter* remounter_;
  signature::SignatureManager* sig_mgr_;
};

#endif  // CVMFS_NOTIFICATION_CLIENT_H_
