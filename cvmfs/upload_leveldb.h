/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_LEVELDB_H_
#define CVMFS_UPLOAD_LEVELDB_H_

#include <sys/stat.h>

#include <string>
#include <vector>

#include "atomic.h"
#include "leveldb/db.h"
#include "leveldb/options.h"
#include "upload_facility.h"
#include "util_concurrency.h"

namespace upload {

struct LevelDbStreamHandle : public UploadStreamHandle {
  LevelDbStreamHandle(const CallbackTN   *commit_callback,
                      const int           tmp_fd,
                      const std::string  &tmp_path)
    : UploadStreamHandle(commit_callback)
    , file_descriptor(tmp_fd)
    , temporary_path(tmp_path) {}

  const int         file_descriptor;
  const std::string temporary_path;
};

class LevelDbHandle {
 public:
  explicit LevelDbHandle(leveldb::DB *database) : database_(database) { }

  void Close() { delete database_; database_ = NULL; }
  leveldb::DB* operator->() const { return database_; }

 private:
  leveldb::DB *database_;
};

/**
 * The LevelDbUploader implements the AbstractUploader interface to push files
 * into one or multiple LevelDB.
 * For a detailed description of the classes interface please have a look into
 * the AbstractUploader base class.
 */
class LevelDbUploader : public AbstractUploader {
 protected:
  typedef std::vector<LevelDbHandle> LevelDbHandles;

 public:
  explicit LevelDbUploader(const SpoolerDefinition &spooler_definition);
  ~LevelDbUploader();

  static bool WillHandle(const SpoolerDefinition &spooler_definition);

  bool Initialize();

  inline std::string name() const { return "LevelDB"; }

  void FileUpload(const std::string  &local_path,
                  const std::string  &remote_path,
                  const CallbackTN   *callback = NULL);

  UploadStreamHandle* InitStreamedUpload(const CallbackTN *callback = NULL);
  void StreamedUpload(UploadStreamHandle  *handle,
                      CharBuffer          *buffer,
                      const CallbackTN    *callback = NULL);
  void FinalizeStreamedUpload(UploadStreamHandle  *handle,
                              const shash::Any    &content_hash);

  bool Remove(const std::string &path);

  bool Peek(const std::string& path) const;

  bool PlaceBootstrappingShortcut(const shash::Any &object) const;

  /**
   * Determines the number of failed jobs in the LocalCompressionWorker as
   * well as in the Upload() command.
   */
  unsigned int GetNumberOfErrors() const;

 private:
  bool ParseConfiguration(const std::string &config_path);
  bool OpenDatabases();
  void CloseDatabases();

  LevelDbHandle& GetDatabaseForPath(const std::string &path) const;
  int PutFile(const std::string &local_path, const std::string &remote_path);

  friend class LevelDbUploaderTestWrapper;

 private:
  std::string               base_path_;
  unsigned                  database_count_;
  leveldb::CompressionType  compression_;

  mutable LevelDbHandles    databases_;
};


/**
 * For unit-testing purposes only!
 */
class LevelDbUploaderTestWrapper {
 public:
  static LevelDbHandle& GetLevelDbHandleForPath(LevelDbUploader    *uploader,
                                                const std::string  &remote_path)
  {
    return uploader->GetDatabaseForPath(remote_path);
  }
};

}  // namespace upload

#endif  // CVMFS_UPLOAD_LEVELDB_H_
