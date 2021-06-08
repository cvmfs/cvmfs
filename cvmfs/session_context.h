/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SESSION_CONTEXT_H_
#define CVMFS_SESSION_CONTEXT_H_

#include <string>
#include <vector>

#include "pack.h"
#include "repository_tag.h"
#include "util/pointer.h"
#include "util_concurrency.h"

namespace upload {

struct CurlSendPayload {
  const std::string* json_message;
  ObjectPackProducer* pack_serializer;
  size_t index;
};

size_t SendCB(void* ptr, size_t size, size_t nmemb, void* userp);
size_t RecvCB(void* buffer, size_t size, size_t nmemb, void* userp);

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

// By default, the maximum number of queued jobs is limited to 10,
// representing 10 * 200 MB = 2GB max memory used by the queue
bool Initialize(const std::string& api_url, const std::string& session_token,
                  const std::string& key_id, const std::string& secret,
                  uint64_t max_pack_size = ObjectPack::kDefaultLimit,
                  uint64_t max_queue_size = 10);
  bool Finalize(bool commit, const std::string& old_root_hash,
                const std::string& new_root_hash,
                const RepositoryTag& tag);

  void WaitForUpload() {};

  ObjectPack::BucketHandle NewBucket();

  bool CommitBucket(const ObjectPack::BucketContentType type,
                    const shash::Any& id, const ObjectPack::BucketHandle handle,
                    const std::string& name = "",
                    const bool force_dispatch = false);

 protected:
  virtual bool InitializeDerived(uint64_t max_queue_size) = 0;

  virtual bool FinalizeDerived() = 0;

  virtual bool Commit(const std::string& old_root_hash,
                      const std::string& new_root_hash,
                      const RepositoryTag& tag) = 0;

  virtual Future<bool>* DispatchObjectPack(ObjectPack* pack) = 0;

  int64_t NumJobsSubmitted() const;

  FifoChannel<Future<bool>*> upload_results_;

  std::string api_url_;
  std::string session_token_;
  std::string key_id_;
  std::string secret_;

 private:
  void Dispatch();

  uint64_t max_pack_size_;

  std::vector<ObjectPack::BucketHandle> active_handles_;

  ObjectPack* current_pack_;
  pthread_mutex_t current_pack_mtx_;

  mutable atomic_int64 objects_dispatched_;
  uint64_t bytes_committed_;
  uint64_t bytes_dispatched_;

  bool initialized_;
};

class SessionContext : public SessionContextBase {
 public:
  SessionContext();

 protected:
  struct UploadJob {
    ObjectPack* pack;
    Future<bool>* result;
  };

  virtual bool InitializeDerived(uint64_t max_queue_size);

  virtual bool FinalizeDerived();

  virtual bool Commit(const std::string& old_root_hash,
                      const std::string& new_root_hash,
                      const RepositoryTag& tag);

  virtual Future<bool>* DispatchObjectPack(ObjectPack* pack);

  virtual bool DoUpload(const UploadJob* job);

 private:
  static void* UploadLoop(void* data);

  UniquePtr<FifoChannel<UploadJob*> > upload_jobs_;

  pthread_t worker_;

  static UploadJob terminator_;
};

}  // namespace upload

#endif  // CVMFS_SESSION_CONTEXT_H_
