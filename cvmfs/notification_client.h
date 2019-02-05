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

/**
 * Notification system client running inside the mountpoint process
 *
 * This class implements a client for the repository notification system, meant
 * to run inside the mountpoint process. Constructor arguments are:
 * @param config - configuration string to connect to the notification system
 * (currently the URL of the notification server)
 * @param repo_name - name of the repository associated with the mount point
 * @param remounter - a pointer to a FuseRemounter object; upon receiving valid
 * notifications about repository activity, a remount is triggered
 * @param sig_mgr - a pointer to a SignatureManager object used to verify
 * messages received from the notification system
 */
class NotificationClient {
 public:
  NotificationClient(const std::string& config, const std::string& repo_name,
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
