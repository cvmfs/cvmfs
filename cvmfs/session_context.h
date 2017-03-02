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
  SessionContext()
      : api_url_(),
        session_token_(),
        drop_lease_(true),
        active_handles_(),
        current_pack_(NULL),
        mtx_() {}
  ~SessionContext();

  bool Initialize(const std::string& api_url, const std::string& session_token,
                  bool drop_lease = true);
  bool FinalizeSession();

  bool DispatchCurrent();

  ObjectPack::BucketHandle NewBucket();

  ObjectPack* current_pack() { return current_pack_; }

 private:
  std::string api_url_;
  std::string session_token_;
  bool drop_lease_;

  std::vector<ObjectPack::BucketHandle> active_handles_;
  ObjectPack* current_pack_;

  pthread_mutex_t mtx_;
};

}  // namespace upload

#endif  // CVMFS_SESSION_CONTEXT_H_
