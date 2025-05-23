/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_GATEWAY_H_
#define CVMFS_UPLOAD_GATEWAY_H_

#include <string>

#include "pack.h"
#include "repository_tag.h"
#include "session_context.h"
#include "upload_facility.h"
#include "util/atomic.h"

namespace upload {

struct GatewayStreamHandle : public UploadStreamHandle {
  GatewayStreamHandle(const CallbackTN *commit_callback,
                      ObjectPack::BucketHandle bkt);

  ObjectPack::BucketHandle bucket;
};

class GatewayUploader : public AbstractUploader {
 public:
  struct Config {
    Config() : session_token_file(), key_file(), api_url() { }
    Config(const std::string &session_token_file, const std::string &key_file,
           const std::string &api_url)
        : session_token_file(session_token_file)
        , key_file(key_file)
        , api_url(api_url) { }
    std::string session_token_file;
    std::string key_file;
    std::string api_url;
  };

  static bool WillHandle(const SpoolerDefinition &spooler_definition);

  static bool ParseSpoolerDefinition(
      const SpoolerDefinition &spooler_definition, Config *config);

  explicit GatewayUploader(const SpoolerDefinition &spooler_definition);

  virtual ~GatewayUploader();

  virtual bool Initialize();

  // Can't "create" a repository storage area with the gateway backend
  virtual bool Create();

  virtual bool FinalizeSession(bool commit, const std::string &old_root_hash,
                               const std::string &new_root_hash,
                               const RepositoryTag &tag);

  virtual void WaitForUpload() const;

  virtual std::string name() const;

  virtual bool Peek(const std::string &path);

  virtual bool Mkdir(const std::string &path);

  virtual bool PlaceBootstrappingShortcut(const shash::Any &object);

  virtual unsigned int GetNumberOfErrors() const;

 protected:
  virtual void DoUpload(const std::string &remote_path,
                        IngestionSource *source,
                        const CallbackTN *callback);

  virtual UploadStreamHandle *InitStreamedUpload(const CallbackTN *callback);

  virtual void StreamedUpload(UploadStreamHandle *handle, UploadBuffer buffer,
                              const CallbackTN *callback);

  virtual void FinalizeStreamedUpload(UploadStreamHandle *handle,
                                      const shash::Any &content_hash);

  virtual void DoRemoveAsync(const std::string &file_to_delete);

 protected:
  virtual void ReadSessionTokenFile(const std::string &token_file_name,
                                    std::string *token);

  virtual bool ReadKey(const std::string &key_file, std::string *key_id,
                       std::string *secret);

  virtual int64_t DoGetObjectSize(const std::string &file_name);

 private:
  void BumpErrors() const;

  Config config_;
  SessionContext *session_context_;
  mutable atomic_int32 num_errors_;
};

}  // namespace upload

#endif  // CVMFS_UPLOAD_GATEWAY_H_
