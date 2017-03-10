/**
 * This file is part of the CernVM File System.
 */

#include "session_context.h"

namespace upload {

SessionContextBase::SessionContextBase()
    : upload_results_(1000, 1000),
      api_url_(),
      session_token_(),
      drop_lease_(true),
      max_pack_size_(ObjectPack::kDefaultLimit),
      current_pack_(NULL),
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

  // Reset internal counters
  stats_ = Stats();

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
  {
    MutexLockGuard lock(current_pack_mtx_);

    if (current_pack_ && current_pack_->GetNoObjects() > 0) {
      atomic_inc64(&stats_.objects_dispatched);
      stats_.bytes_dispatched += current_pack_->size();
      DispatchObjectPack(current_pack_);
      current_pack_ = NULL;
    }
  }

  bool results = true;
  while (!upload_results_.IsEmpty() ||
         (stats_.jobs_finished < NumJobsSubmitted())) {
    Future<bool>* future = upload_results_.Dequeue();
    results = future->Get() && results;
    delete future;
    stats_.jobs_finished++;
  }

  if (drop_lease_ && !DropLease()) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "SessionContext finalization - Could not drop active lease");
  }

  results = FinalizeDerived() &&
            (stats_.bytes_committed == stats_.bytes_dispatched) && results;

  pthread_mutex_destroy(&current_pack_mtx_);
  return results;
}

ObjectPack::BucketHandle SessionContextBase::NewBucket() {
  MutexLockGuard lock(current_pack_mtx_);
  ObjectPack::BucketHandle hd = CurrentPack()->NewBucket();
  stats_.buckets_created++;
  return hd;
}

bool SessionContextBase::CommitBucket(const ObjectPack::BucketContentType type,
                                      const shash::Any& id,
                                      const ObjectPack::BucketHandle handle,
                                      const std::string& name,
                                      const bool force_dispatch) {
  MutexLockGuard lock(current_pack_mtx_);

  uint64_t size0 = CurrentPack()->size();
  bool committed = CurrentPack()->CommitBucket(type, id, handle, name);

  if (committed) {  // Current pack is still not full
    uint64_t size1 = CurrentPack()->size();
    stats_.buckets_committed++;
    stats_.bytes_committed += size1 - size0;
    if (force_dispatch) {
      Dispatch();
      current_pack_ = NULL;
    }
  } else {  // Current pack is full and can be dispatched
    ObjectPack* new_pack = new ObjectPack(max_pack_size_);
    CurrentPack()->TransferBucket(handle, new_pack);

    Dispatch();
    current_pack_ = new_pack;

    CommitBucket(type, id, handle, name, false);
  }

  return true;
}

SessionContextBase::Stats SessionContextBase::stats() const { return stats_; }

int64_t SessionContextBase::NumJobsSubmitted() const {
  return atomic_read64(&stats_.objects_dispatched);
}

ObjectPack* SessionContextBase::CurrentPack() {
  MutexLockGuard lock(current_pack_mtx_);
  if (!current_pack_) {
    current_pack_ = new ObjectPack(max_pack_size_);
  }
  return current_pack_;
}

void SessionContextBase::Dispatch() {
  atomic_inc64(&stats_.objects_dispatched);
  stats_.bytes_dispatched += CurrentPack()->size();
  upload_results_.Enqueue(DispatchObjectPack(CurrentPack()));
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
  LogCvmfs(kLogUploadGateway, kLogStderr,
           "Upload session context - worker started with context @%ld", data);

  int64_t jobs_processed = 0;
  while (!ctx->ShouldTerminate()) {
    while (jobs_processed < ctx->NumJobsSubmitted()) {
      UploadJob* job = ctx->upload_jobs_.Dequeue();
      ctx->DoUpload(job);
      jobs_processed++;
    }
  }

  return NULL;
}

bool SessionContext::ShouldTerminate() {
  return atomic_read32(&worker_terminate_);
}

}  // namespace upload
