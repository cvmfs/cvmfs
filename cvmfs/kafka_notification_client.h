/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_KAFKA_NOTIFICATION_CLIENT_H_
#define CVMFS_KAFKA_NOTIFICATION_CLIENT_H_

#include <string>

#include "download.h"
#include "fuse_remount.h"
#include "notify/subscriber.h"
#include "util/pointer.h"
#include "util/single_copy.h"

namespace signature {
class SignatureManager;
}

/**
 * Notification system client running inside the mountpoint process
 *
 * This class implements a client for the repository notification system, meant
 * to run inside the mountpoint process. 
 * Derivative of CVMFS's existing NotificationClient
 */

class KafkaNotificationClient : public SingleCopy {
 public:
// broker, username, password, topic
  KafkaNotificationClient(const std::string& broker, const std::string& username, const std::string& password, const std::string& topic, const std::string& repo_name,
                     FuseRemounter* remounter,
                     download::DownloadManager* dl_mgr,
                     signature::SignatureManager* sig_mgr);
  virtual ~KafkaNotificationClient();

  void Spawn();

 private:
  static void* Run(void* instance);

	int Consume(const std::string& repo_name, const std::string& msg_text) ;
	std::string broker_;
	std::string username_;
	std::string password_;
	std::string topic_;
  std::string config_;
  std::string repo_name_;
  FuseRemounter* remounter_;
  download::DownloadManager* dl_mgr_;
  signature::SignatureManager* sig_mgr_;
  UniquePtr<notify::Subscriber> subscriber_;
  pthread_t thread_;
	bool shutdown_;
  bool spawned_;
};

#endif  // CVMFS_NOTIFICATION_CLIENT_H_
