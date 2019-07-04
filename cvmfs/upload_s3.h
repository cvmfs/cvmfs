/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_S3_H_
#define CVMFS_UPLOAD_S3_H_

#include <pthread.h>

#include <string>
#include <utility>
#include <vector>

#include "atomic.h"
#include "s3fanout.h"
#include "upload_facility.h"

namespace upload {

struct S3StreamHandle : public UploadStreamHandle {
  S3StreamHandle(
    const CallbackTN *commit_callback,
    const int tmp_fd,
    const std::string &tmp_path)
    : UploadStreamHandle(commit_callback)
    , file_descriptor(tmp_fd)
    , temporary_path(tmp_path)
  { }

  const int file_descriptor;
  const std::string temporary_path;
};

/**
 * The S3Spooler implements the AbstractSpooler interface to push files
 * into a S3 CVMFS repository backend.
 * For a detailed description of the classes interface please have a look into
 * the AbstractSpooler base class.
 */
class S3Uploader : public AbstractUploader {
 public:
  explicit S3Uploader(const SpoolerDefinition &spooler_definition);
  virtual ~S3Uploader();
  static bool WillHandle(const SpoolerDefinition &spooler_definition);

  virtual std::string name() const { return "S3"; }

  /**
   * Upload() is not done concurrently in the current implementation of the
   * S3Spooler, since it is a simple move or copy of a file without CPU
   * intensive operation
   * This method calls NotifyListeners and invokes a callback for all
   * registered listeners (see the Observable template for details).
   */
  virtual void FileUpload(const std::string &local_path,
                          const std::string &remote_path,
                          const CallbackTN *callback = NULL);

  virtual UploadStreamHandle *InitStreamedUpload(
    const CallbackTN *callback = NULL);
  virtual void StreamedUpload(UploadStreamHandle *handle, UploadBuffer buffer,
                              const CallbackTN *callback = NULL);
  virtual void FinalizeStreamedUpload(UploadStreamHandle *handle,
                                      const shash::Any &content_hash);

  virtual void DoRemoveAsync(const std::string &file_to_delete);
  virtual bool Peek(const std::string &path);
  virtual bool PlaceBootstrappingShortcut(const shash::Any &object);

  virtual unsigned int GetNumberOfErrors() const;
  int64_t DoGetObjectSize(const std::string &file_name);

  // Only for testing
  s3fanout::S3FanoutManager *GetS3FanoutManager() { return s3fanout_mgr_; }

 private:
  static const unsigned kDefaultPort = 80;
  static const unsigned kDefaultNumParallelUploads = 16;
  static const unsigned kDefaultNumRetries = 3;
  static const unsigned kDefaultTimeoutSec = 60;
  static const unsigned kDefaultBackoffInitMs = 100;
  static const unsigned kDefaultBackoffMaxMs = 2000;

  // Used to make the async HEAD requests synchronous in Peek()
  struct PeekCtrl {
    PeekCtrl() : exists(false) { pipe_wait[0] = pipe_wait[1] = 0; }
    bool exists;
    int pipe_wait[2];
  };

  void OnPeekComplete(const upload::UploaderResults &results, PeekCtrl *ctrl);

  static void *MainCollectResults(void *data);

  bool ParseSpoolerDefinition(const SpoolerDefinition &spooler_definition);
  void UploadJobInfo(s3fanout::JobInfo *info);

  s3fanout::JobInfo *CreateJobInfo(const std::string &path) const;

  s3fanout::S3FanoutManager *s3fanout_mgr_;
  std::string repository_alias_;
  std::string host_name_port_;
  std::string host_name_;
  std::string region_;
  std::string bucket_;
  bool dns_buckets_;
  int num_parallel_uploads_;
  unsigned num_retries_;
  unsigned timeout_sec_;
  std::string access_key_;
  std::string secret_key_;
  s3fanout::AuthzMethods authz_method_;
  bool peek_before_put_;

  const std::string temporary_path_;
  mutable atomic_int32 io_errors_;
  /**
   * Signals the CollectResults thread to quit
   */
  atomic_int32 terminate_;
  pthread_t thread_collect_results_;
};  // S3Uploader

}  // namespace upload

#endif  // CVMFS_UPLOAD_S3_H_
