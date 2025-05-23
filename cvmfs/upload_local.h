/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_LOCAL_H_
#define CVMFS_UPLOAD_LOCAL_H_

#include <sys/stat.h>
#include <unistd.h>

#include <string>

#include "upload_facility.h"
#include "util/atomic.h"
#include "util/concurrency.h"

namespace upload {

struct LocalStreamHandle : public UploadStreamHandle {
  LocalStreamHandle(const CallbackTN *commit_callback, const int tmp_fd,
                    const std::string &tmp_path)
      : UploadStreamHandle(commit_callback)
      , file_descriptor(tmp_fd)
      , temporary_path(tmp_path) { }

  const int file_descriptor;
  const std::string temporary_path;
};

/**
 * The LocalSpooler implements the AbstractSpooler interface to push files
 * into a local CVMFS repository backend.
 * For a detailed description of the classes interface please have a look into
 * the AbstractSpooler base class.
 */
class LocalUploader : public AbstractUploader {
 private:
  static const mode_t default_backend_file_mode_ = 0666;
  static const mode_t default_backend_dir_mode_ = 0777;
  const mode_t backend_file_mode_;
  const mode_t backend_dir_mode_;

 public:
  explicit LocalUploader(const SpoolerDefinition &spooler_definition);
  static bool WillHandle(const SpoolerDefinition &spooler_definition);

  virtual std::string name() const { return "Local"; }

  virtual bool Create();

  /**
   * Upload() is not done concurrently in the current implementation of the
   * LocalSpooler, since it is a simple move or copy of a file without CPU
   * intensive operation
   * This method calls NotifyListeners and invokes a callback for all
   * registered listeners (see the Observable template for details).
   */
  void DoUpload(const std::string &remote_path,
                IngestionSource *source,
                const CallbackTN *callback);

  UploadStreamHandle *InitStreamedUpload(const CallbackTN *callback);
  void StreamedUpload(UploadStreamHandle *handle, UploadBuffer buffer,
                      const CallbackTN *callback = NULL);
  void FinalizeStreamedUpload(UploadStreamHandle *handle,
                              const shash::Any &content_hash);

  void DoRemoveAsync(const std::string &file_to_delete);

  bool Peek(const std::string &path);

  bool Mkdir(const std::string &path);

  bool PlaceBootstrappingShortcut(const shash::Any &object);

  /**
   * Determines the number of failed jobs in the LocalCompressionWorker as
   * well as in the Upload() command.
   */
  unsigned int GetNumberOfErrors() const;

  int64_t DoGetObjectSize(const std::string &file_name);

 protected:
  int Move(const std::string &local_path, const std::string &remote_path) const;

 private:
  // state information
  const std::string upstream_path_;
  const std::string temporary_path_;
  mutable atomic_int32 copy_errors_;  //!< counts the number of occurred
                                      //!< errors in Upload()
};

}  // namespace upload

#endif  // CVMFS_UPLOAD_LOCAL_H_
