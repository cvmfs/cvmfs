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
#include "util/file_backed_buffer.h"
#include "util/pointer.h"
#include "util/single_copy.h"

namespace upload {

struct S3StreamHandle : public UploadStreamHandle {
  S3StreamHandle(
    const CallbackTN *commit_callback,
    uint64_t in_memory_threshold,
    const std::string &tmp_dir = "/tmp/")
    : UploadStreamHandle(commit_callback)
  {
    buffer = FileBackedBuffer::Create(in_memory_threshold, tmp_dir);
  }

  // Ownership is later transferred to the S3 fanout
  UniquePtr<FileBackedBuffer> buffer;
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

  virtual bool Create();

  /**
   * Upload() is not done concurrently in the current implementation of the
   * S3Spooler, since it is a simple move or copy of a file without CPU
   * intensive operation
   * This method calls NotifyListeners and invokes a callback for all
   * registered listeners (see the Observable template for details).
   */
  virtual void DoUpload(const std::string &remote_path,
                        IngestionSource *source,
                        const CallbackTN *callback = NULL);

  virtual UploadStreamHandle *InitStreamedUpload(
    const CallbackTN *callback = NULL);
  virtual void StreamedUpload(UploadStreamHandle *handle, UploadBuffer buffer,
                              const CallbackTN *callback = NULL);
  virtual void FinalizeStreamedUpload(UploadStreamHandle *handle,
                                      const shash::Any &content_hash);

  virtual void DoRemoveAsync(const std::string &file_to_delete);
  virtual bool Peek(const std::string &path);
  virtual bool Mkdir(const std::string &path);
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
  static const unsigned kInMemoryObjectThreshold = 500*1024;  // 500KiB

  // Used to make the async HTTP requests synchronous in Peek() Create(),
  // and Upload() of single bits
  struct RequestCtrl : SingleCopy {
    RequestCtrl() : return_code(-1), callback_forward(NULL) {
      pipe_wait[0] = pipe_wait[1] = -1;
    }

    void WaitFor();

    int return_code;
    const CallbackTN *callback_forward;
    std::string original_path;
    int pipe_wait[2];
  };

  void OnReqComplete(const upload::UploaderResults &results, RequestCtrl *ctrl);

  static void *MainCollectResults(void *data);

  bool ParseSpoolerDefinition(const SpoolerDefinition &spooler_definition);
  void UploadJobInfo(s3fanout::JobInfo *info);

  s3fanout::JobInfo *CreateJobInfo(const std::string &path) const;

  UniquePtr<s3fanout::S3FanoutManager> s3fanout_mgr_;
  std::string repository_alias_;
  std::string host_name_port_;
  std::string host_name_;
  std::string region_;
  std::string flavor_;
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
  pthread_t thread_collect_results_;
};  // S3Uploader

}  // namespace upload

#endif  // CVMFS_UPLOAD_S3_H_
