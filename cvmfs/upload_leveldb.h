/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_LEVELDB_H_
#define CVMFS_UPLOAD_LEVELDB_H_

#include <sys/stat.h>

#include <string>

#include "atomic.h"
#include "upload_facility.h"
#include "util_concurrency.h"

namespace upload {

struct LevelDbStreamHandle : public UploadStreamHandle {
  LevelDbStreamHandle(const CallbackTN *commit_callback) :
    UploadStreamHandle(commit_callback) {}
};

/**
 * The LevelDbUploader implements the AbstractUploader interface to push files
 * into one or multiple LevelDB.
 * For a detailed description of the classes interface please have a look into
 * the AbstractUploader base class.
 */
class LevelDbUploader : public AbstractUploader {
 public:
  explicit LevelDbUploader(const SpoolerDefinition &spooler_definition);
  static bool WillHandle(const SpoolerDefinition &spooler_definition);

  inline std::string name() const { return "LevelDB"; }

  void FileUpload(const std::string  &local_path,
                  const std::string  &remote_path,
                  const CallbackTN   *callback = NULL);

  UploadStreamHandle* InitStreamedUpload(const CallbackTN *callback = NULL);
  void Upload(UploadStreamHandle  *handle,
              CharBuffer          *buffer,
              const CallbackTN    *callback = NULL);
  void FinalizeStreamedUpload(UploadStreamHandle  *handle,
                              const shash::Any    &content_hash);

  bool Remove(const std::string &file_to_delete);

  bool Peek(const std::string& path) const;

  bool PlaceBootstrappingShortcut(const shash::Any &object) const;

  /**
   * Determines the number of failed jobs in the LocalCompressionWorker as
   * well as in the Upload() command.
   */
  unsigned int GetNumberOfErrors() const;

 protected:
  void WorkerThread();
};

}  // namespace upload

#endif  // CVMFS_UPLOAD_LEVELDB_H_
