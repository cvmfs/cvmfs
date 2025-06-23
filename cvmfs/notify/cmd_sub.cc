/**
 * This file is part of the CernVM File System.
 */

#ifndef __STDC_FORMAT_MACROS
// NOLINTNEXTLINE
#define __STDC_FORMAT_MACROS
#endif

#include "cmd_sub.h"

#include <inttypes.h>

#include "crypto/signature.h"
#include "manifest.h"
#include "manifest_fetch.h"
#include "network/download.h"
#include "notify/messages.h"
#include "options.h"
#include "subscriber_sse.h"
#include "subscriber_supervisor.h"
#include "supervisor.h"
#include "util/logging.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/string.h"

namespace {

const LogFacilities &kLogInfo = DefaultLogging::info;
const LogFacilities &kLogError = DefaultLogging::error;

const int kMaxPoolHandles = 1;

class SwissknifeSubscriber : public notify::SubscriberSSE {
 public:
  SwissknifeSubscriber(const std::string &server_url,
                       const std::string &repository, uint64_t min_revision,
                       bool continuous, bool verbose)
      : notify::SubscriberSSE(server_url)
      , repository_(repository)
      , stats_()
      , dl_mgr_(new download::DownloadManager(
            kMaxPoolHandles, perf::StatisticsTemplate("download", &stats_)))
      , sig_mgr_(new signature::SignatureManager())
      , revision_(min_revision)
      , continuous_(continuous)
      , verbose_(verbose) { }
  virtual ~SwissknifeSubscriber() { sig_mgr_->Fini(); }

  bool Init() {
    const std::string config_file = "/etc/cvmfs/repositories.d/" + repository_
                                    + "/client.conf";
    SimpleOptionsParser options;
    if (!options.TryParsePath(config_file)) {
      LogCvmfs(kLogCvmfs, kLogError,
               "SwissknifeSubscriber - could not parse configuration file");
      return false;
    }

    std::string arg;
    if (options.GetValue("CVMFS_SERVER_URL", &arg)) {
      dl_mgr_->SetHostChain(arg);
    }

    sig_mgr_->Init();

    const std::string public_keys = JoinStrings(
        FindFilesBySuffix("/etc/cvmfs/keys", ".pub"), ":");
    if (!sig_mgr_->LoadPublicRsaKeys(public_keys)) {
      LogCvmfs(kLogCvmfs, kLogError,
               "SwissknifeSubscriber - could not load public keys");
      return false;
    }

    return true;
  }

 private:
  virtual notify::Subscriber::Status Consume(const std::string &repo,
                                             const std::string &msg_text) {
    notify::msg::Activity msg;
    if (!msg.FromJSONString(msg_text)) {
      LogCvmfs(kLogCvmfs, kLogError,
               "SwissknifeSubscriber - could not decode message.");
      return notify::Subscriber::kError;
    }

    manifest::ManifestEnsemble ensemble;
    const manifest::Failures res = manifest::Verify(
        reinterpret_cast<unsigned char *>(&(msg.manifest_[0])),
        msg.manifest_.size(), "", repo, 0, NULL, sig_mgr_.weak_ref(),
        dl_mgr_.weak_ref(), &ensemble);

    if (res != manifest::kFailOk) {
      LogCvmfs(kLogCvmfs, kLogError,
               "SwissknifeSubscriber - manifest has invalid signature: %d",
               res);
      return notify::Subscriber::kError;
    }

    const UniquePtr<manifest::Manifest> manifest(manifest::Manifest::LoadMem(
        reinterpret_cast<const unsigned char *>(msg.manifest_.data()),
        msg.manifest_.size()));

    if (!manifest.IsValid()) {
      LogCvmfs(kLogCvmfs, kLogError,
               "SwissknifeSubscriber - could not parse manifest.");
      return notify::Subscriber::kError;
    }

    const uint64_t new_revision = manifest->revision();
    bool triggered = false;
    if (new_revision > revision_) {
      LogCvmfs(
          kLogCvmfs, kLogInfo,
          "SwissknifeSubscriber - repository %s is now at revision %" PRIu64
          ".",
          repo.c_str(), new_revision);
      if (verbose_) {
        LogCvmfs(kLogCvmfs, kLogInfo, "%s", msg_text.c_str());
      }
      revision_ = new_revision;
      triggered = true;
    }

    if (!continuous_ && triggered) {
      return notify::Subscriber::kFinish;
    }

    return notify::Subscriber::kContinue;
  }

  std::string repository_;

  perf::Statistics stats_;
  UniquePtr<download::DownloadManager> dl_mgr_;
  UniquePtr<signature::SignatureManager> sig_mgr_;

  uint64_t revision_;
  bool continuous_;
  bool verbose_;
};

}  // namespace

namespace notify {

int DoSubscribe(const std::string &server_url, const std::string &repo,
                uint64_t min_revision, bool continuous, bool verbose) {
  SwissknifeSubscriber subscriber(server_url, repo, min_revision, continuous,
                                  verbose);

  if (!subscriber.Init()) {
    LogCvmfs(kLogCvmfs, kLogError, "Could not initialize SwissknifeSubscriber");
    return 1;
  }

  // Retry settings: accept no more than 10 failures in the last minute
  const int num_retries = 10;
  const uint64_t interval = 60;
  SubscriberSupervisor supervisor(&subscriber, repo, num_retries, interval);
  supervisor.Run();

  return 0;
}

}  // namespace notify
