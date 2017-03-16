/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SESSION_CONTEXT_H_
#define CVMFS_SESSION_CONTEXT_H_

#include <string>
#include <vector>

#include "pack.h"
#include "util_concurrency.h"

namespace upload {

/**
 * This class implements a context for a single publish operation
 *
 * The context is created at the start of a publish operation and
 * is supposed to live at least until the payload has been submitted
 * to the repo services.
 *
 * It is the GatewayUploader concrete class which handles the creation and
 * destruction of the SessionContext. A session should begin when the spooler
 * and uploaders are initialized and should last until the call to
 * Spooler::WaitForUpload().
 */
class SessionContextBase {
 public:
  SessionContextBase();

  virtual ~SessionContextBase();

  bool Initialize(const std::string& api_url, const std::string& session_token,
                  bool drop_lease = true,
                  uint64_t max_pack_size = ObjectPack::kDefaultLimit);
  bool Finalize();

  void WaitForUpload();

  ObjectPack::BucketHandle NewBucket();

  bool CommitBucket(const ObjectPack::BucketContentType type,
                    const shash::Any& id, const ObjectPack::BucketHandle handle,
                    const std::string& name = "",
                    const bool force_dispatch = false);

 protected:
  virtual bool InitializeDerived() = 0;

  virtual bool FinalizeDerived() = 0;

  virtual bool DropLease() = 0;

  virtual Future<bool>* DispatchObjectPack(ObjectPack* pack) = 0;

  int64_t NumJobsSubmitted() const;

  FifoChannel<Future<bool>*> upload_results_;

  std::string api_url_;
  std::string session_token_;
  bool drop_lease_;

  FifoChannel<bool> queue_was_flushed_;

 private:
  void Dispatch();

  uint64_t max_pack_size_;

  std::vector<ObjectPack::BucketHandle> active_handles_;

  ObjectPack* current_pack_;
  pthread_mutex_t current_pack_mtx_;

  mutable atomic_int64 objects_dispatched_;
  uint64_t bytes_committed_;
  uint64_t bytes_dispatched_;
};

class SessionContext : public SessionContextBase {
 public:
  SessionContext();

 protected:
  struct UploadJob {
    ObjectPack* pack;
    Future<bool>* result;
  };

  virtual bool InitializeDerived();

  virtual bool FinalizeDerived();

  virtual bool DropLease();

  virtual Future<bool>* DispatchObjectPack(ObjectPack* pack);

  virtual bool DoUpload(const UploadJob* job);

 private:
  static void* UploadLoop(void* data);

  bool ShouldTerminate();

  FifoChannel<UploadJob*> upload_jobs_;

  atomic_int32 worker_terminate_;
  pthread_t worker_;
};

}  // namespace upload

#endif  // CVMFS_SESSION_CONTEXT_H_
