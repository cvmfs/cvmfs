/**
 * This file is part of the CernVM File System.
 */

#ifndef __STDC_FORMAT_MACROS
// NOLINTNEXTLINE
#define __STDC_FORMAT_MACROS
#endif

#include "notification_client.h"

#include <inttypes.h>

#include <string>
#include <vector>

#include "crypto/signature.h"
#include "manifest.h"
#include "manifest_fetch.h"
#include "notify/messages.h"
#include "notify/subscriber_sse.h"
#include "notify/subscriber_supervisor.h"
#include "supervisor.h"
#include "util/logging.h"
#include "util/posix.h"

namespace {

class ActivitySubscriber : public notify::SubscriberSSE {
 public:
  ActivitySubscriber(const std::string &server_url, FuseRemounter *remounter,
                     download::DownloadManager *dl_mgr,
                     signature::SignatureManager *sig_mgr)
      : SubscriberSSE(server_url)
      , remounter_(remounter)
      , dl_mgr_(dl_mgr)
      , sig_mgr_(sig_mgr) { }

  virtual ~ActivitySubscriber() { }

  virtual notify::Subscriber::Status Consume(const std::string &repo_name,
                                             const std::string &msg_text) {
    notify::msg::Activity msg;
    if (!msg.FromJSONString(msg_text)) {
      LogCvmfs(kLogCvmfs, kLogSyslogErr,
               "NotificationClient - could not decode message.");
      return notify::Subscriber::kError;
    }

    manifest::ManifestEnsemble ensemble;
    const manifest::Failures res = manifest::Verify(
        reinterpret_cast<unsigned char *>(&(msg.manifest_[0])),
        msg.manifest_.size(), "", repo_name, 0, NULL, sig_mgr_, dl_mgr_,
        &ensemble);

    if (res != manifest::kFailOk) {
      LogCvmfs(kLogCvmfs, kLogSyslogErr,
               "NotificationClient - manifest has invalid signature.");
      return notify::Subscriber::kError;
    }

    const UniquePtr<manifest::Manifest> manifest(manifest::Manifest::LoadMem(
        reinterpret_cast<const unsigned char *>(msg.manifest_.data()),
        msg.manifest_.size()));

    if (!manifest.IsValid()) {
      LogCvmfs(kLogCvmfs, kLogSyslogErr,
               "NotificationClient - could not parse manifest.");
      return notify::Subscriber::kError;
    }

    const uint64_t new_revision = manifest->revision();
    LogCvmfs(kLogCvmfs, kLogSyslog,
             "NotificationClient - repository %s is now at revision %" PRIu64
             ", root hash: %s",
             repo_name.c_str(), new_revision,
             manifest->catalog_hash().ToString().c_str());

    const FuseRemounter::Status status = remounter_->CheckSynchronously();
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
      case FuseRemounter::kStatusMaintenance:
        LogCvmfs(kLogCvmfs, kLogSyslog,
                 "NotificationClient - in maintenance mode");
        break;
      default:
        LogCvmfs(kLogCvmfs, kLogSyslog, "NotificationClient - internal error");
    }
    return notify::Subscriber::kContinue;
  }

 private:
  FuseRemounter *remounter_;
  download::DownloadManager *dl_mgr_;
  signature::SignatureManager *sig_mgr_;
};

}  // namespace

NotificationClient::NotificationClient(const std::string &config,
                                       const std::string &repo_name,
                                       FuseRemounter *remounter,
                                       download::DownloadManager *dl_mgr,
                                       signature::SignatureManager *sig_mgr)
    : config_(config)
    , repo_name_(repo_name)
    , remounter_(remounter)
    , dl_mgr_(dl_mgr)
    , sig_mgr_(sig_mgr)
    , subscriber_()
    , thread_()
    , spawned_(false) { }

NotificationClient::~NotificationClient() {
  if (subscriber_.IsValid()) {
    subscriber_->Unsubscribe();
  }
  if (spawned_) {
    pthread_join(thread_, NULL);
    spawned_ = false;
  }
}

void NotificationClient::Spawn() {
  if (!spawned_) {
    if (pthread_create(&thread_, NULL, NotificationClient::Run, this)) {
      LogCvmfs(kLogCvmfs, kLogSyslogErr,
               "NotificationClient - Could not start background thread");
    }
    spawned_ = true;
  }
}

void *NotificationClient::Run(void *data) {
  NotificationClient *cl = static_cast<NotificationClient *>(data);

  cl->subscriber_ = new ActivitySubscriber(cl->config_, cl->remounter_,
                                           cl->dl_mgr_, cl->sig_mgr_);

  LogCvmfs(
      kLogCvmfs, kLogSyslog,
      "NotificationClient - Entering subscription loop for repository: %s.",
      cl->repo_name_.c_str());

  // Retry settings: accept no more than 10 failures in the last minute
  const int num_retries = 10;
  const uint64_t interval = 60;
  notify::SubscriberSupervisor supervisor(
      cl->subscriber_.weak_ref(), cl->repo_name_, num_retries, interval);
  supervisor.Run();

  return NULL;
}
