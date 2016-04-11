/**
 * This file is part of the CernVM File System.
 */

#include "upload_leveldb.h"

#include "file_processing/char_buffer.h"
#include "hash.h"
#include "options.h"
#include "util/posix.h"
#include "util/string.h"
#include "util/mmap_file.h"

namespace upload {

LevelDbUploader::LevelDbUploader(const SpoolerDefinition &spooler_definition) :
  AbstractUploader(spooler_definition)
{
  assert(spooler_definition.IsValid() &&
         spooler_definition.driver_type == SpoolerDefinition::LevelDb);
}

LevelDbUploader::~LevelDbUploader() {
  CloseDatabases();
}

bool LevelDbUploader::WillHandle(const SpoolerDefinition &spooler_definition) {
  return spooler_definition.driver_type == SpoolerDefinition::LevelDb;
}


bool LevelDbUploader::Initialize() {
  const SpoolerDefinition  &sd          = spooler_definition();
  const std::string         config_path = sd.spooler_configuration;

  return ParseConfiguration(config_path) &&
         OpenDatabases()                 &&
         AbstractUploader::Initialize();
}


bool LevelDbUploader::ParseConfiguration(const std::string &config_path) {
  if (!FileExists(config_path)) {
    LogCvmfs(kLogUploadLevelDb, kLogStderr,
             "LevelDB uploader config file not found at '%s'",
             config_path.c_str());
    return false;
  }

  BashOptionsManager options_manager;
  options_manager.ParsePath(config_path, false);

  if (!options_manager.GetValue("CVMFS_LEVELDB_STORAGE", &base_path_)) {
    LogCvmfs(kLogUploadLevelDb, kLogStderr,
             "Failed to parse CVMFS_LEVELDB_STORAGE from '%s'",
             config_path.c_str());
    return false;
  }

  std::string leveldb_count_str;
  if (!options_manager.GetValue("CVMFS_LEVELDB_COUNT", &leveldb_count_str)) {
    LogCvmfs(kLogUploadLevelDb, kLogStderr,
             "Failed to parse CVMFS_LEVELDB_COUNT from '%s'",
             config_path.c_str());
    return false;
  }

  database_count_ = String2Uint64(leveldb_count_str);
  if (database_count_ < 1 || database_count_ > 255) {
    LogCvmfs(kLogUploadLevelDb, kLogStderr,
             "Fail, invalid CVMFS_LEVELDB_COUNT given: '%s'.",
             leveldb_count_str.c_str());
    LogCvmfs(kLogUploadLevelDb, kLogStderr,
             "CVMFS_LEVELDB_COUNT should be in range 1-255.");
    return false;
  }

  std::string compression;
  if (!options_manager.GetValue("CVMFS_LEVELDB_COMPRESSION", &compression)) {
    compression = "none";  // default
  }

  if (compression == "none") {
    compression_ = leveldb::kNoCompression;
  } else if (compression == "snappy") {
    compression_ = leveldb::kSnappyCompression;
  } else {
    LogCvmfs(kLogUploadLevelDb, kLogStderr,
             "unknown compression method '%s' in CVMFS_LEVELDB_COMPRESSION",
             compression.c_str());
    return false;
  }

  return true;
}


bool LevelDbUploader::OpenDatabases() {
  assert(database_count_ > 0 && database_count_ < 1000);

  for (unsigned i = 0; i < database_count_; ++i) {
    const std::string db_name = "lvldb" + PaddingLeft(StringifyInt(i), 3, '0');
    const std::string db_path = base_path_ + "/" + db_name;

    leveldb::DB      *database;
    leveldb::Options  options;
    options.create_if_missing = true;
    options.error_if_exists   = false;
    options.paranoid_checks   = false;
    options.compression       = compression_;
    const leveldb::Status status = leveldb::DB::Open(options,
                                                     db_path,
                                                     &database);
    if (!status.ok()) {
      LogCvmfs(kLogUploadLevelDb, kLogStderr,
               "failed to open LevelDB '%s' - %s",
               db_path.c_str(), status.ToString().c_str());
      return false;
    }

    databases_.push_back(LevelDbHandle(database));
  }

  return true;
}


void LevelDbUploader::CloseDatabases() {
  LevelDbHandles::iterator       i    = databases_.begin();
  LevelDbHandles::const_iterator iend = databases_.end();
  for (; i != iend; ++i) {
    i->Close();
  }
  databases_.clear();
}


LevelDbHandle& LevelDbUploader::GetDatabaseForPath(
                                                const std::string &path) const {
  shash::Any h(shash::kMd5);
  HashString(path, &h);
  const uint32_t shorthash = *reinterpret_cast<uint32_t*>(h.digest);
  return databases_[shorthash % database_count_];
}


unsigned int LevelDbUploader::GetNumberOfErrors() const {
  return 0;  // TODO(rmeusel): implement me
}


int LevelDbUploader::PutFile(const std::string &local_path,
                              const std::string &remote_path) {
  LevelDbHandle& handle = GetDatabaseForPath(remote_path);

  MemoryMappedFile file(local_path);
  if (!file.Map()) {
    LogCvmfs(kLogUploadLevelDb, kLogStderr, "failed to read '%s'",
             local_path.c_str());
    return errno;
  }

  leveldb::WriteOptions options;
  const leveldb::Status status =
    handle->Put(options,
                remote_path,
                leveldb::Slice(reinterpret_cast<const char*>(file.buffer()),
                               file.size()));
  if (!status.ok()) {
    LogCvmfs(kLogUploadLevelDb, kLogStderr, "failed to write '%s' to LevelDB",
             local_path.c_str());
    return 2;
  }

  return 0;
}


void LevelDbUploader::FileUpload(
  const std::string &local_path,
  const std::string &remote_path,
  const CallbackTN   *callback
) {
  const int retcode = PutFile(local_path, remote_path);
  Respond(callback, UploaderResults(retcode, local_path));
}


UploadStreamHandle* LevelDbUploader::InitStreamedUpload(
                                                   const CallbackTN *callback) {
  std::string tmp_path;
  const int tmp_fd = CreateAndOpenTemporaryChunkFile(&tmp_path);

  if (tmp_fd < 0) {
    LogCvmfs(kLogUploadS3, kLogStderr, "Failed to open file (%d), %s",
             errno, strerror(errno));
    return NULL;
  }

  return new LevelDbStreamHandle(callback, tmp_fd, tmp_path);
}


void LevelDbUploader::StreamedUpload(UploadStreamHandle  *handle,
                                     CharBuffer          *buffer,
                                     const CallbackTN    *callback) {
  assert(buffer->IsInitialized());
  LevelDbStreamHandle *lvldb_handle = static_cast<LevelDbStreamHandle*>(handle);

  const size_t bytes_written = write(lvldb_handle->file_descriptor,
                                     buffer->ptr(),
                                     buffer->used_bytes());
  if (bytes_written != buffer->used_bytes()) {
    const int write_errno = errno;
    LogCvmfs(kLogUploadS3, kLogStderr, "failed to write %d bytes to '%s' "
                                       "(errno: %d)",
             buffer->used_bytes(), lvldb_handle->temporary_path.c_str(),
             write_errno);
    Respond(callback, UploaderResults(write_errno, buffer));
    return;
  }

  Respond(callback, UploaderResults(0, buffer));
}


void LevelDbUploader::FinalizeStreamedUpload(UploadStreamHandle *handle,
                                             const shash::Any   &content_hash) {
  LevelDbStreamHandle *lvldb_handle = static_cast<LevelDbStreamHandle*>(handle);
  const CallbackTN    *callback     = handle->commit_callback;

  int retval = close(lvldb_handle->file_descriptor);
  if (retval != 0) {
    const int cpy_errno = errno;
    LogCvmfs(kLogUploadLevelDb, kLogStderr, "failed to close temp file '%s' "
                                            "(errno: %d)",
             lvldb_handle->temporary_path.c_str(), cpy_errno);
    Respond(handle->commit_callback, UploaderResults(cpy_errno));
    return;
  }

  const std::string final_path("data/" + content_hash.MakePath());
  const int retcode = PutFile(lvldb_handle->temporary_path, final_path);

  retval = unlink(lvldb_handle->temporary_path.c_str());
  assert(retval == 0);
  delete lvldb_handle;

  Respond(callback, UploaderResults(retcode));
}


bool LevelDbUploader::Remove(const std::string& path) {
  LevelDbHandle& handle = GetDatabaseForPath(path);

  leveldb::WriteOptions write_options;
  leveldb::Status status = handle->Delete(write_options, path);

  if (!status.ok()) {
    LogCvmfs(kLogUploadLevelDb, kLogStderr, "failed to delete '%s' in LevelDB",
             path.c_str());
    return false;
  }

  return true;
}


bool LevelDbUploader::Peek(const std::string& path) const {
  LevelDbHandle& handle = GetDatabaseForPath(path);

  leveldb::ReadOptions read_options;
  read_options.fill_cache = false;
  std::string buffer;
  leveldb::Status status = handle->Get(read_options, path, &buffer);

  if (!status.ok() && !status.IsNotFound()) {
    LogCvmfs(kLogUploadLevelDb, kLogStderr, "failed to peek '%s' in LevelDB",
             path.c_str());
    return false;
  }

  return status.ok();
}


bool LevelDbUploader::PlaceBootstrappingShortcut(const shash::Any &object) const {
  return false;
}


}  // namespace upload
