/**
 * This file is part of the CernVM File System.
 */

#include "cmd_sub.h"

#include "logging.h"
#include "manifest.h"
#include "notify/messages.h"
#include "signature.h"
#include "subscriber_supervisor.h"
#include "subscriber_ws.h"
#include "supervisor.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/string.h"

namespace {

const LogFacilities& kLogInfo = DefaultLogging::info;
const LogFacilities& kLogError = DefaultLogging::error;

class TriggerSubscriber : public notify::SubscriberWS {
 public:
  TriggerSubscriber(const std::string& server_url, uint64_t min_revision,
                    bool continuous, bool verbose)
      : notify::SubscriberWS(server_url),
        revision_(min_revision),
        continuous_(continuous),
        verbose_(verbose) {}
  virtual ~TriggerSubscriber() {}

 private:
  virtual bool Consume(const std::string& repo, const std::string& msg_text) {
    notify::msg::Activity msg;
    if (!msg.FromJSONString(msg_text)) {
      LogCvmfs(kLogCvmfs, kLogError, "Could not decode message.");
      return false;
    }

    signature::SignatureManager sig_mgr;

    std::string cert_path = "/etc/cvmfs/keys/" + repo + ".crt";
    if (!sig_mgr.LoadCertificatePath(cert_path)) {
      LogCvmfs(kLogCvmfs, kLogError, "Could not load repository certificate.");
      return false;
    }

    if (!sig_mgr.VerifyLetter(
            reinterpret_cast<const unsigned char*>(msg.manifest_.data()),
            msg.manifest_.size(), false)) {
      LogCvmfs(kLogCvmfs, kLogError, "Manifest has invalid signature.");
      return false;
    }

    const UniquePtr<manifest::Manifest> manifest(manifest::Manifest::LoadMem(
        reinterpret_cast<const unsigned char*>(msg.manifest_.data()),
        msg.manifest_.size()));

    if (!manifest.IsValid()) {
      LogCvmfs(kLogCvmfs, kLogError, "Could not parse manifest.");
      return false;
    }

    uint64_t new_revision = manifest->revision();
    bool triggered = false;
    if (new_revision > revision_) {
      LogCvmfs(kLogCvmfs, kLogInfo, "Repository %s is now at revision %lu.",
               repo.c_str(), new_revision, revision_);
      if (verbose_) {
        LogCvmfs(kLogCvmfs, kLogInfo, "%s", msg_text.c_str());
      }
      revision_ = new_revision;
      triggered = true;
    }

    if (!continuous_ && triggered) {
      return false;
    }

    return true;
  }

  uint64_t revision_;
  bool continuous_;
  bool verbose_;
};

}  // namespace

namespace notify {

int DoSubscribe(const std::string& server_url, const std::string& repo,
                uint64_t min_revision, bool continuous, bool verbose) {
  TriggerSubscriber subscriber(server_url, min_revision, continuous, verbose);

  // Retry settings: accept no more than 10 failures in the last minute
  const int num_retries = 10;
  const uint64_t interval = 60;
  SubscriberSupervisor supervisor(&subscriber, repo, num_retries, interval);
  supervisor.Run();

  return 0;
}

}  // namespace notify
