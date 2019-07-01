/**
 * This file is part of the CernVM File System.
 */

#include "subscriber_supervisor.h"

#include "logging.h"
#include "subscriber.h"

namespace {

const LogFacilities& kLogError = DefaultLogging::error;

}  // namespace

namespace notify {

SubscriberSupervisor::SubscriberSupervisor(notify::Subscriber* s, std::string t,
                                           int max_retries, uint64_t interval)
    : Supervisor(max_retries, interval), subscriber_(s), topic_(t) {}

SubscriberSupervisor::~SubscriberSupervisor() {}

bool SubscriberSupervisor::Task() {
  bool ret = subscriber_->Subscribe(topic_);
  if (ret) {
    LogCvmfs(
        kLogCvmfs, kLogDebug,
        "SubscriberSupervisor - Subscription ended successfully. Stopping.");
  } else {
    LogCvmfs(kLogCvmfs, kLogError,
             "SubscriberSupervisor - Subscription failed. Retrying.");
  }
  return ret;
}

}  // namespace notify
