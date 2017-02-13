/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_HTTP_H_
#define CVMFS_UPLOAD_HTTP_H_

#include <string>

#include "upload_facility.h"

namespace upload {

class HttpUploader : public AbstractUploader {
 public:
  struct Config {
    std::string repository_address;
    uint16_t port;
    std::string api_path;
  };

  explicit HttpUploader(const SpoolerDefinition& spooler_definition);
  static bool WillHandle(const SpoolerDefinition& spooler_definition);

  virtual ~HttpUploader();

  virtual UploadStreamHandle* InitStreamedUpload(
      const CallbackTN* callback = NULL);

  virtual bool Remove(const std::string& file_to_delete);

  virtual bool Peek(const std::string& path) const;

  virtual bool PlaceBootstrappingShortcut(const shash::Any& object) const;

  virtual unsigned int GetNumberOfErrors() const;

  static bool ParseSpoolerDefinition(
      const SpoolerDefinition& spooler_definition, Config* config);

 protected:
  virtual void FileUpload(const std::string& local_path,
                          const std::string& remote_path,
                          const CallbackTN* callback = NULL);

  virtual void StreamedUpload(UploadStreamHandle* handle, CharBuffer* buffer,
                              const CallbackTN* callback);

  virtual void FinalizeStreamedUpload(UploadStreamHandle* handle,
                                      const shash::Any& content_hash);

 private:
  Config config_;
};

}  // namespace upload

#endif  // CVMFS_UPLOAD_HTTP_H_
