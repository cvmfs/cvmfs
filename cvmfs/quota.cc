/**
 * This file is part of the CernVM File System.
 */


#include "quota.h"

#include <errno.h>
#include <unistd.h>

#include <cstdlib>

#include "util/concurrency.h"
#include "util/logging.h"
#include "util/smalloc.h"

using namespace std;  // NOLINT

const uint32_t QuotaManager::kProtocolRevision = 2;

void QuotaManager::BroadcastBackchannels(const string &message) {
  assert(message.length() > 0);
  MutexLockGuard lock_guard(*lock_back_channels_);

  for (map<shash::Md5, int>::iterator i = back_channels_.begin(),
                                      iend = back_channels_.end();
       i != iend;) {
    LogCvmfs(kLogQuota, kLogDebug, "broadcasting %s to %s", message.c_str(),
             i->first.ToString().c_str());
    int written = write(i->second, message.data(), message.length());
    if (written < 0)
      written = 0;
    if (static_cast<unsigned>(written) != message.length()) {
      bool remove_backchannel = errno != EAGAIN;
      LogCvmfs(kLogQuota, kLogDebug | kLogSyslogWarn,
               "failed to broadcast '%s' to %s (written %d, error %d)",
               message.c_str(), i->first.ToString().c_str(), written, errno);
      if (remove_backchannel) {
        LogCvmfs(kLogQuota, kLogDebug | kLogSyslogWarn,
                 "removing back channel %s", i->first.ToString().c_str());
        map<shash::Md5, int>::iterator remove_me = i;
        ++i;
        close(remove_me->second);
        back_channels_.erase(remove_me);
      } else {
        ++i;
      }
    } else {
      ++i;
    }
  }
}


QuotaManager::QuotaManager() : protocol_revision_(0) {
  lock_back_channels_ = reinterpret_cast<pthread_mutex_t *>(
      smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock_back_channels_, NULL);
  assert(retval == 0);
}


QuotaManager::~QuotaManager() {
  for (map<shash::Md5, int>::iterator i = back_channels_.begin(),
                                      iend = back_channels_.end();
       i != iend;
       ++i) {
    close(i->second);
  }
  pthread_mutex_destroy(lock_back_channels_);
  free(lock_back_channels_);
}
