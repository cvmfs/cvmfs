/**
 * This file is part of the CernVM File System.
 */

#include "session_context.h"

#include <algorithm>

namespace upload {

SessionContextBase::SessionContextBase()
    : upload_results_(1000, 1000),
      api_url_(),
      session_token_(),
      drop_lease_(true),
      max_pack_size_(ObjectPack::kDefaultLimit),
      active_handles_(),
      current_pack_(NULL),
      current_pack_mtx_() {}

SessionContextBase::~SessionContextBase() {}

bool SessionContextBase::Initialize(const std::string& api_url,
                                    const std::string& session_token,
                                    bool drop_lease, uint64_t max_pack_size) {
  bool ret = true;

  // Initialize session context lock
  pthread_mutexattr_t attr;
  if (pthread_mutexattr_init(&attr) ||
      pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE) ||
      pthread_mutex_init(&current_pack_mtx_, &attr) ||
      pthread_mutexattr_destroy(&attr)) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "Could not initialize SessionContext lock.");
    return false;
  }

  // Set upstream URL and session token
  api_url_ = api_url;
  session_token_ = session_token;
  drop_lease_ = drop_lease;
  max_pack_size_ = max_pack_size;

  atomic_init64(&objects_dispatched_);
  bytes_committed_ = 0u;
  bytes_dispatched_ = 0u;

  // Ensure that the upload job and result queues are empty
  if (!upload_results_.IsEmpty()) {
    LogCvmfs(
        kLogUploadGateway, kLogStderr,
        "Could not initialize SessionContext - Upload queues are not empty.");
    ret = false;
  }

  // Ensure that there are not open object packs
  if (current_pack_) {
    LogCvmfs(
        kLogUploadGateway, kLogStderr,
        "Could not initialize SessionContext - Existing open object packs.");
    ret = false;
  }

  ret = InitializeDerived() && ret;

  return ret;
}

bool SessionContextBase::Finalize() {
  assert(active_handles_.empty());
  {
    MutexLockGuard lock(current_pack_mtx_);

    if (current_pack_ && current_pack_->GetNoObjects() > 0) {
      Dispatch();
      current_pack_ = NULL;
    }
  }

  bool results = true;
  int64_t jobs_finished = 0;
  while (!upload_results_.IsEmpty() || (jobs_finished < NumJobsSubmitted())) {
    Future<bool>* future = upload_results_.Dequeue();
    results = future->Get() && results;
    delete future;
    jobs_finished++;
  }

  if (drop_lease_ && !DropLease()) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "SessionContext finalization - Could not drop active lease");
  }

  results =
      FinalizeDerived() && (bytes_committed_ == bytes_dispatched_) && results;

  pthread_mutex_destroy(&current_pack_mtx_);
  return results;
}

ObjectPack::BucketHandle SessionContextBase::NewBucket() {
  MutexLockGuard lock(current_pack_mtx_);
  if (!current_pack_) {
    current_pack_ = new ObjectPack(max_pack_size_);
  }
  ObjectPack::BucketHandle hd = current_pack_->NewBucket();
  active_handles_.push_back(hd);
  return hd;
}

bool SessionContextBase::CommitBucket(const ObjectPack::BucketContentType type,
                                      const shash::Any& id,
                                      const ObjectPack::BucketHandle handle,
                                      const std::string& name,
                                      const bool force_dispatch) {
  MutexLockGuard lock(current_pack_mtx_);

  if (!current_pack_) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "Error: Called SessionBaseContext::CommitBucket without an open "
             "ObjectPack.");
    return false;
  }

  uint64_t size0 = current_pack_->size();
  bool committed = current_pack_->CommitBucket(type, id, handle, name);

  if (committed) {  // Current pack is still not full
    active_handles_.erase(
        std::remove(active_handles_.begin(), active_handles_.end(), handle),
        active_handles_.end());
    uint64_t size1 = current_pack_->size();
    bytes_committed_ += size1 - size0;
    if (force_dispatch) {
      Dispatch();
      current_pack_ = NULL;
    }
  } else {  // Current pack is full and can be dispatched
    ObjectPack* new_pack = new ObjectPack(max_pack_size_);
    for (size_t i = 0u; i < active_handles_.size(); ++i) {
      current_pack_->TransferBucket(active_handles_[i], new_pack);
    }

    Dispatch();
    current_pack_ = new_pack;

    CommitBucket(type, id, handle, name, false);
  }

  return true;
}

int64_t SessionContextBase::NumJobsSubmitted() const {
  return atomic_read64(&objects_dispatched_);
}

void SessionContextBase::Dispatch() {
  MutexLockGuard lock(current_pack_mtx_);

  if (!current_pack_) {
    return;
  }

  atomic_inc64(&objects_dispatched_);
  bytes_dispatched_ += current_pack_->size();
  upload_results_.Enqueue(DispatchObjectPack(current_pack_));
}

SessionContext::SessionContext()
    : SessionContextBase(), upload_jobs_(1000, 900) {}

bool SessionContext::InitializeDerived() {
  // Start worker thread
  atomic_init32(&worker_terminate_);
  atomic_write32(&worker_terminate_, 0);
  int retval =
      pthread_create(&worker_, NULL, UploadLoop, reinterpret_cast<void*>(this));

  return !retval;
}

bool SessionContext::FinalizeDerived() {
  atomic_write32(&worker_terminate_, 1);

  pthread_join(worker_, NULL);

  return true;
}

bool SessionContext::DropLease() { return true; }

Future<bool>* SessionContext::DispatchObjectPack(const ObjectPack* pack) {
  UploadJob* job = new UploadJob;
  job->pack = pack;
  job->result = new Future<bool>();
  upload_jobs_.Enqueue(job);
  return job->result;
}

bool SessionContext::DoUpload(const SessionContext::UploadJob* /*job*/) {
  return true;
}

void* SessionContext::UploadLoop(void* data) {
  SessionContext* ctx = reinterpret_cast<SessionContext*>(data);

  int64_t jobs_processed = 0;
  while (!ctx->ShouldTerminate()) {
    while (jobs_processed < ctx->NumJobsSubmitted()) {
      UploadJob* job = ctx->upload_jobs_.Dequeue();
      ctx->DoUpload(job);
      delete job->pack;
      delete job;
      job->result->Set(true);
      jobs_processed++;
    }
  }

  return NULL;
}

bool SessionContext::ShouldTerminate() {
  return atomic_read32(&worker_terminate_);
}

}  // namespace upload
