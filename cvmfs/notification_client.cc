/**
 * This file is part of the CernVM File System.
 */

#include "notification_client.h"

#include <string>
#include <vector>

#include "logging.h"
#include "manifest.h"
#include "notify/messages.h"
#include "notify/subscriber_supervisor.h"
#include "notify/subscriber_ws.h"
#include "signature.h"
#include "supervisor.h"
#include "util/posix.h"

namespace {

class ActivitySubscriber : public notify::SubscriberWS {
 public:
  ActivitySubscriber(const std::string& server_url, FuseRemounter* remounter,
                     signature::SignatureManager* sig_mgr)
      : SubscriberWS(server_url), remounter_(remounter), sig_mgr_(sig_mgr) {}

  virtual ~ActivitySubscriber() {}

  virtual bool Consume(const std::string& repo_name,
                       const std::string& msg_text) {
    notify::msg::Activity msg;
    if (!msg.FromJSONString(msg_text)) {
      LogCvmfs(kLogCvmfs, kLogSyslogErr,
               "ActivitySubscriber - Could not decode message.");
      return false;
    }

    if (!sig_mgr_->VerifyLetter(
            reinterpret_cast<const unsigned char*>(msg.manifest_.data()),
            msg.manifest_.size(), false)) {
      LogCvmfs(kLogCvmfs, kLogSyslogErr, "Manifest has invalid signature.");
      return false;
    }

    const UniquePtr<manifest::Manifest> manifest(manifest::Manifest::LoadMem(
        reinterpret_cast<const unsigned char*>(msg.manifest_.data()),
        msg.manifest_.size()));

    if (!manifest.IsValid()) {
      LogCvmfs(kLogCvmfs, kLogSyslogErr,
               "ActivitySubscriber - Could not parse manifest.");
      return false;
    }

    uint64_t new_revision = manifest->revision();
    LogCvmfs(kLogCvmfs, kLogSyslog,
             "Repository %s is now at revision %lu, root hash: %s",
             repo_name.c_str(), new_revision,
             manifest->catalog_hash().ToString().c_str());

    FuseRemounter::Status status = remounter_->CheckSynchronously();
    switch (status) {
      case FuseRemounter::kStatusFailGeneral:
        LogCvmfs(kLogCvmfs, kLogSyslog, "NotificationClient - remount failed");
        break;
      case FuseRemounter::kStatusFailNoSpace:
        LogCvmfs(kLogCvmfs, kLogSyslog,
                 "NotificationClient - remount failed (no space)");
        break;
      case FuseRemounter::kStatusUp2Date:
        LogCvmfs(kLogCvmfs, kLogSyslog,
                 "NotificationClient - catalog up to date");
        break;
      case FuseRemounter::kStatusDraining:
        LogCvmfs(kLogCvmfs, kLogSyslog,
                 "NotificationClient - new revision applied");
        break;
      case FuseRemounter::kStatusMaintenance:
        LogCvmfs(kLogCvmfs, kLogSyslog,
                 "NotificationClient - in maintenance mode");
        break;
      default:
        LogCvmfs(kLogCvmfs, kLogSyslog, "NotificationClient - internal error");
    }

    return true;
  }

 private:
  FuseRemounter* remounter_;
  signature::SignatureManager* sig_mgr_;
};

}  // namespace

NotificationClient::NotificationClient(const std::string& config,
                                       const std::string& repo_name,
                                       FuseRemounter* remounter,
                                       signature::SignatureManager* sig_mgr)
    : config_(config),
      repo_name_(repo_name),
      remounter_(remounter),
      sig_mgr_(sig_mgr) {}

NotificationClient::~NotificationClient() {}

void NotificationClient::Spawn() {
  pthread_t th;
  if (pthread_create(&th, NULL, NotificationClient::Run, this)) {
    LogCvmfs(kLogCvmfs, kLogSyslogErr,
             "ActivitySubscriber - Could not start background thread");
  }
  pthread_detach(th);
}

void* NotificationClient::Run(void* data) {
  NotificationClient* cl = static_cast<NotificationClient*>(data);

  UniquePtr<ActivitySubscriber> sub(
      new ActivitySubscriber(cl->config_, cl->remounter_, cl->sig_mgr_));

  LogCvmfs(
      kLogCvmfs, kLogSyslog,
      "NotificationClient - Entering subscription loop for repository: %s.",
      cl->repo_name_.c_str());

  // Retry settings: accept no more than 10 failures in the last minute
  const int num_retries = 10;
  const uint64_t interval = 60;
  notify::SubscriberSupervisor supervisor(sub.weak_ref(), cl->repo_name_,
                                          num_retries, interval);
  supervisor.Run();

  return NULL;
}
