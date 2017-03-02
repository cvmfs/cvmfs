/**
 * This file is part of the CernVM File System.
 */

#include "session_context.h"

#include "util_concurrency.h"

namespace upload {

SessionContext::~SessionContext() {}

bool SessionContext::Initialize(const std::string& api_url,
                                const std::string& session_token,
                                bool drop_lease) {
  // Add checks on api_url and session_token ?
  api_url_ = api_url;
  session_token_ = session_token;
  drop_lease_ = drop_lease;

  return !pthread_mutex_init(&mtx_, NULL);
}

bool SessionContext::FinalizeSession() { return true; }

bool SessionContext::DispatchCurrent() { return true; }

ObjectPack::BucketHandle SessionContext::NewBucket() {
  MutexLockGuard lock(mtx_);
  if (!current_pack_) {
    current_pack_ = new ObjectPack;
  }

  ObjectPack::BucketHandle hd = current_pack_->NewBucket();
  active_handles_.push_back(hd);
  return hd;
}

}  // namespace upload
