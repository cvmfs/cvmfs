/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_HTTP_H_
#define CVMFS_UPLOAD_HTTP_H_

#include <atomic.h>

#include <string>

#include "session_context.h"
#include "upload_facility.h"

namespace upload {

struct HttpStreamHandle : public UploadStreamHandle {
  explicit HttpStreamHandle(const CallbackTN* commit_callback);
};

class HttpUploader : public AbstractUploader {
 public:
  struct Config {
    Config()
        : user_name(),
          repository_subpath(),
          repository_address(),
          api_path(),
          port(0) {}
    Config(const std::string& user_name, const std::string& repo_subpath,
           const std::string& repository_address, const std::string& api_path,
           const uint16_t port)
        : user_name(user_name),
          repository_subpath(repo_subpath),
          repository_address(repository_address),
          api_path(api_path),
          port(port) {}
    std::string user_name;
    std::string repository_subpath;
    std::string repository_address;
    std::string api_path;
    uint16_t port;
  };

  static bool ParseSpoolerDefinition(
      const SpoolerDefinition& spooler_definition, Config* config);

  static bool WillHandle(const SpoolerDefinition& spooler_definition);

  explicit HttpUploader(const SpoolerDefinition& spooler_definition);

  virtual ~HttpUploader();

  virtual bool Initialize();

  virtual bool FinalizeSession();

  virtual std::string name() const;

  virtual UploadStreamHandle* InitStreamedUpload(
      const CallbackTN* callback = NULL);

  virtual bool Remove(const std::string& file_to_delete);

  virtual bool Peek(const std::string& path) const;

  virtual bool PlaceBootstrappingShortcut(const shash::Any& object) const;

  virtual unsigned int GetNumberOfErrors() const;

 protected:
  virtual void FileUpload(const std::string& local_path,
                          const std::string& remote_path,
                          const CallbackTN* callback = NULL);

  virtual void StreamedUpload(UploadStreamHandle* handle, CharBuffer* buffer,
                              const CallbackTN* callback);

  virtual void FinalizeStreamedUpload(UploadStreamHandle* handle,
                                      const shash::Any& content_hash);

 private:
  void BumpErrors() const;

  Config config_;
  UniquePtr<SessionContext> session_context_;
  mutable atomic_int32 num_errors_;
};

}  // namespace upload

#endif  // CVMFS_UPLOAD_HTTP_H_
