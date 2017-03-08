/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SESSION_CONTEXT_H_
#define CVMFS_SESSION_CONTEXT_H_

#include <string>

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
  struct Stats {
    Stats()
        : buckets_created(0u),
          buckets_committed(0u),
          objects_dispatched(0u),
          jobs_finished(0u),
          bytes_committed(0u),
          bytes_dispatched(0u) {}

    uint64_t buckets_created;
    uint64_t buckets_committed;
    uint64_t objects_dispatched;
    uint64_t jobs_finished;
    uint64_t bytes_committed;
    uint64_t bytes_dispatched;
  };

  SessionContextBase();

  virtual ~SessionContextBase();

  bool Initialize(const std::string& api_url, const std::string& session_token,
                  bool drop_lease = true,
                  uint64_t max_pack_size = ObjectPack::kDefaultLimit);
  bool Finalize();

  ObjectPack::BucketHandle NewBucket();

  bool CommitBucket(const ObjectPack::BucketContentType type,
                    const shash::Any& id, const ObjectPack::BucketHandle handle,
                    const std::string& name = "",
                    const bool force_dispatch = false);

  Stats stats() const { return stats_; }

 protected:
  virtual bool InitializeDerived();

  virtual bool FinalizeDerived();

  virtual bool DropLease();

  virtual Future<bool>* DispatchObjectPack(ObjectPack* pack) = 0;

 private:
  ObjectPack* CurrentPack();

  std::string api_url_;
  std::string session_token_;
  bool drop_lease_;

  uint64_t max_pack_size_;

  ObjectPack* current_pack_;

  pthread_mutex_t mtx_;

  FifoChannel<Future<bool>*> upload_results_;

  Stats stats_;
};

class SessionContext : public SessionContextBase {
 public:
  SessionContext();

 protected:
  virtual Future<bool>* DispatchObjectPack(ObjectPack* pack);

 private:
  struct UploadJob;
  FifoChannel<UploadJob*> upload_jobs_;
};

}  // namespace upload

#endif  // CVMFS_SESSION_CONTEXT_H_
