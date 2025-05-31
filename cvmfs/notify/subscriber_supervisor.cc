/**
 * This file is part of the CernVM File System.
 */

#include "subscriber_supervisor.h"

#include "subscriber.h"
#include "util/logging.h"

namespace {

const LogFacilities &kLogInfo = DefaultLogging::info;
const LogFacilities &kLogError = DefaultLogging::error;

}  // namespace

namespace notify {

SubscriberSupervisor::SubscriberSupervisor(notify::Subscriber *s, std::string t,
                                           int max_retries, uint64_t interval)
    : Supervisor(max_retries, interval), subscriber_(s), topic_(t) { }

SubscriberSupervisor::~SubscriberSupervisor() { }

bool SubscriberSupervisor::Task() {
  const bool ret = subscriber_->Subscribe(topic_);
  if (ret) {
    LogCvmfs(
        kLogCvmfs, kLogInfo,
        "SubscriberSupervisor - Subscription ended successfully. Stopping.");
  } else {
    LogCvmfs(kLogCvmfs, kLogError,
             "SubscriberSupervisor - Subscription failed. Retrying.");
  }
  return ret;
}

}  // namespace notify
