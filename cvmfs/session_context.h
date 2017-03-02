/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SESSION_CONTEXT_H_
#define CVMFS_SESSION_CONTEXT_H_

#include <string>
#include <vector>

#include "pack.h"

namespace upload {

/**
 * This class implements a context for a single publish operation
 *
 * The context is created at the start of a publish operation and
 * is supposed to live at least until the payload has been submitted
 * to the repo services.
 *
 * It is the HttpUploader concrete class which handles the creation and
 * destruction of the SessionContext. A session should begin when the spooler
 * and uploaders are initialized and should last until the call to
 * Spooler::WaitForUpload().
 */
class SessionContext {
 public:
  struct Stats {
    Stats()
        : buckets_created(0u),
          buckets_committed(0u),
          objects_dispatched(0u),
          bytes_committed(0u),
          bytes_dispatched(0u) {}

    uint64_t buckets_created;
    uint64_t buckets_committed;
    uint64_t objects_dispatched;
    uint64_t bytes_committed;
    uint64_t bytes_dispatched;
  };

  SessionContext()
      : api_url_(),
        session_token_(),
        drop_lease_(true),
        active_handles_(),
        current_pack_(NULL),
        mtx_(),
        stats_() {}

  virtual ~SessionContext();

  bool Initialize(const std::string& api_url, const std::string& session_token,
                  bool drop_lease = true);
  bool FinalizeSession();

  ObjectPack::BucketHandle NewBucket();

  bool CommitBucket(const ObjectPack::BucketContentType type,
                    const shash::Any& id, const ObjectPack::BucketHandle handle,
                    const std::string& name = "");

  Stats stats() const { return stats_; }

 private:
  ObjectPack* CurrentPack();

  void DispatchIfNeeded();

  std::string api_url_;
  std::string session_token_;
  bool drop_lease_;

  std::vector<ObjectPack::BucketHandle> active_handles_;
  ObjectPack* current_pack_;

  pthread_mutex_t mtx_;

  Stats stats_;
};

}  // namespace upload

#endif  // CVMFS_SESSION_CONTEXT_H_
