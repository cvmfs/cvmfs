/**
 * This file is part of the CernVM File System.
 */

#include "session_context.h"

namespace upload {

SessionContextBase::SessionContextBase()
    : api_url_(),
      session_token_(),
      drop_lease_(true),
      active_handle_(0),
      current_pack_(NULL),
      mtx_(),
      upload_results_(1000, 1000),
      stats_() {}

SessionContextBase::~SessionContextBase() {}

bool SessionContextBase::Initialize(const std::string& api_url,
                                    const std::string& session_token,
                                    bool drop_lease) {
  // Set upstream URL and session token
  api_url_ = api_url;
  session_token_ = session_token;
  drop_lease_ = drop_lease;

  // Reset internal counters
  stats_ = Stats();

  bool ret = true;

  // Initialize session context lock
  if (pthread_mutex_init(&mtx_, NULL)) {
    LogCvmfs(kLogUploadHttp, kLogStderr,
             "Could not initialize SessionContext lock.");
    ret = false;
  }

  // Ensure that the upload job and result queues are empty
  if (!upload_results_.IsEmpty()) {
    LogCvmfs(
        kLogUploadHttp, kLogStderr,
        "Could not initialize SessionContext - Upload queues are not empty.");
    ret = false;
  }

  // Ensure that there are no open buckets
  if (active_handle_) {
    LogCvmfs(kLogUploadHttp, kLogStderr,
             "Could not initialize SessionContext - Existing open buckets.");
    ret = false;
  }

  // Ensure that there are not open object packs
  if (current_pack_) {
    LogCvmfs(
        kLogUploadHttp, kLogStderr,
        "Could not initialize SessionContext - Existing open object packs.");
    ret = false;
  }

  return InitializeDerived() && ret;
}

bool SessionContextBase::Finalize() {
  if (drop_lease_) {
    // TODO: make a request to drop the lease
  }

  bool results = true;
  while (!upload_results_.IsEmpty() ||
         (stats_.jobs_finished < stats_.objects_dispatched)) {
    Future<bool>* future = upload_results_.Dequeue();
    results = future->Get() && results;
    delete future;
    stats_.jobs_finished++;
  }
  return FinalizeDerived() && results &&
         (stats_.bytes_committed == stats_.bytes_dispatched);
}

ObjectPack::BucketHandle SessionContextBase::NewBucket() {
  MutexLockGuard lock(mtx_);
  ObjectPack::BucketHandle hd = CurrentPack()->NewBucket();
  stats_.buckets_created++;
  return hd;
}

bool SessionContextBase::CommitBucket(const ObjectPack::BucketContentType type,
                                      const shash::Any& id,
                                      const ObjectPack::BucketHandle handle,
                                      const std::string& name) {
  MutexLockGuard lock(mtx_);
  active_handle_ = handle;
  uint64_t size0 = CurrentPack()->size();
  bool committed = CurrentPack()->CommitBucket(type, id, handle, name);

  if (committed) {  // Current pack is still not full
    uint64_t size1 = CurrentPack()->size();
    stats_.buckets_committed++;
    stats_.bytes_committed += size1 - size0;
  } else {  // Current pack is full and can be dispatched
    Dispatch();
    CommitBucket(type, id, active_handle_, name);
  }
  return true;
}

void SessionContextBase::Dispatch() {
  ObjectPack* new_pack = new ObjectPack();
  CurrentPack()->TransferBucket(active_handle_, new_pack);

  upload_results_.Enqueue(DispatchObjectPack(CurrentPack()));

  stats_.objects_dispatched++;
  stats_.bytes_dispatched += CurrentPack()->size();

  current_pack_ = new_pack;
}

ObjectPack* SessionContextBase::CurrentPack() {
  if (!current_pack_) {
    current_pack_ = new ObjectPack();
  }
  return current_pack_;
}

struct SessionContext::UploadJob {
  ObjectPack* pack;
  Future<bool>* result;
};

bool SessionContextBase::InitializeDerived() { return true; }

bool SessionContextBase::FinalizeDerived() { return true; }

SessionContext::SessionContext()
    : SessionContextBase(), upload_jobs_(1000, 900) {}

Future<bool>* SessionContext::DispatchObjectPack(ObjectPack* pack) {
  UploadJob* job = new UploadJob;
  job->pack = pack;
  job->result = new Future<bool>();
  upload_jobs_.Enqueue(job);
  return job->result;
}

}  // namespace upload
