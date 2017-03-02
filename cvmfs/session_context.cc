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

  stats_ = Stats();

  return !pthread_mutex_init(&mtx_, NULL);
}

bool SessionContext::FinalizeSession() {
  Stats stats = stats_;
  stats_ = Stats();
  return stats.bytes_committed == stats.bytes_dispatched;
}

ObjectPack::BucketHandle SessionContext::NewBucket() {
  MutexLockGuard lock(mtx_);
  ObjectPack::BucketHandle hd = CurrentPack()->NewBucket();
  active_handles_.push_back(hd);
  stats_.buckets_created++;
  return hd;
}

bool SessionContext::CommitBucket(const ObjectPack::BucketContentType type,
                                  const shash::Any& id,
                                  const ObjectPack::BucketHandle handle,
                                  const std::string& name) {
  MutexLockGuard lock(mtx_);
  uint64_t size0 = CurrentPack()->size();
  bool committed = CurrentPack()->CommitBucket(type, id, handle, name);

  if (committed) {
    uint64_t size1 = CurrentPack()->size();
    stats_.buckets_committed++;
    stats_.bytes_committed += size1 - size0;
    DispatchIfNeeded();
  }
  return committed;
}

ObjectPack* SessionContext::CurrentPack() {
  if (!current_pack_) {
    current_pack_ = new ObjectPack();
  }
  return current_pack_;
}

void SessionContext::DispatchIfNeeded() {}

}  // namespace upload
