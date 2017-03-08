/**
 * This file is part of the CernVM File System.
 */

#include "session_context.h"

namespace upload {

SessionContextBase::SessionContextBase()
    : api_url_(),
      session_token_(),
      drop_lease_(true),
      max_pack_size_(ObjectPack::kDefaultLimit),
      current_pack_(NULL),
      mtx_(),
      upload_results_(1000, 1000),
      stats_() {}

SessionContextBase::~SessionContextBase() {}

bool SessionContextBase::Initialize(const std::string& api_url,
                                    const std::string& session_token,
                                    bool drop_lease, uint64_t max_pack_size) {
  bool ret = true;

  // Initialize session context lock
  pthread_mutexattr_t attr;
  if (pthread_mutexattr_init(&attr) ||
      pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE) ||
      pthread_mutex_init(&mtx_, &attr) || pthread_mutexattr_destroy(&attr)) {
    LogCvmfs(kLogUploadHttp, kLogStderr,
             "Could not initialize SessionContext lock.");
    return false;
  }

  {
    MutexLockGuard lock(mtx_);

    // Set upstream URL and session token
    api_url_ = api_url;
    session_token_ = session_token;
    drop_lease_ = drop_lease;
    max_pack_size_ = max_pack_size;

    // Reset internal counters
    stats_ = Stats();

    // Ensure that the upload job and result queues are empty
    if (!upload_results_.IsEmpty()) {
      LogCvmfs(
          kLogUploadHttp, kLogStderr,
          "Could not initialize SessionContext - Upload queues are not empty.");
      ret = false;
    }

    // Ensure that there are not open object packs
    if (current_pack_) {
      LogCvmfs(
          kLogUploadHttp, kLogStderr,
          "Could not initialize SessionContext - Existing open object packs.");
      ret = false;
    }

    ret = InitializeDerived() && ret;
  }

  return ret;
}

bool SessionContextBase::Finalize() {
  bool results = true;
  {
    MutexLockGuard lock(mtx_);

    if (drop_lease_ && !DropLease()) {
      LogCvmfs(kLogUploadHttp, kLogStderr,
               "SessionContext finalization - Could not drop active lease");
    }

    while (!upload_results_.IsEmpty() ||
           (stats_.jobs_finished < stats_.objects_dispatched)) {
      Future<bool>* future = upload_results_.Dequeue();
      results = future->Get() && results;
      delete future;
      stats_.jobs_finished++;
    }

    results = FinalizeDerived() &&
              (stats_.bytes_committed == stats_.bytes_dispatched) && results;
  }

  pthread_mutex_destroy(&mtx_);
  return results;
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
                                      const std::string& name,
                                      const bool force_dispatch) {
  MutexLockGuard lock(mtx_);

  uint64_t size0 = CurrentPack()->size();
  bool committed = CurrentPack()->CommitBucket(type, id, handle, name);

  if (committed && !force_dispatch) {  // Current pack is still not full
    uint64_t size1 = CurrentPack()->size();
    stats_.buckets_committed++;
    stats_.bytes_committed += size1 - size0;
  } else {  // Current pack is full and can be dispatched
    ObjectPack* new_pack = new ObjectPack(max_pack_size_);
    CurrentPack()->TransferBucket(handle, new_pack);

    upload_results_.Enqueue(DispatchObjectPack(CurrentPack()));

    stats_.objects_dispatched++;
    stats_.bytes_dispatched += CurrentPack()->size();
    current_pack_ = new_pack;

    CommitBucket(type, id, handle, name, false);
  }
  return true;
}

ObjectPack* SessionContextBase::CurrentPack() {
  MutexLockGuard lock(mtx_);
  if (!current_pack_) {
    current_pack_ = new ObjectPack(max_pack_size_);
  }
  return current_pack_;
}

struct SessionContext::UploadJob {
  ObjectPack* pack;
  Future<bool>* result;
};

bool SessionContextBase::InitializeDerived() {
  MutexLockGuard lock(mtx_);
  return true;
}

bool SessionContextBase::FinalizeDerived() {
  MutexLockGuard lock(mtx_);
  return true;
}

bool SessionContextBase::DropLease() {
  MutexLockGuard lock(mtx_);
  return true;
}

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
