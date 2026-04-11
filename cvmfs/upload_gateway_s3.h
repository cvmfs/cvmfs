/**
 * This file is part of the CernVM File System.
 *
 * Prototype: GatewayS3Uploader
 *
 * This uploader extends GatewayUploader so that content-addressed data
 * objects are written directly to S3, bypassing the gateway payload
 * pipeline.  Only catalog objects are posted through the gateway.
 *
 * The S3FanoutManager is initialized from a standard CVMFS S3 config
 * file (the same format used by the native S3 backend).
 */

#ifndef CVMFS_UPLOAD_GATEWAY_S3_H_
#define CVMFS_UPLOAD_GATEWAY_S3_H_

#include <string>

#include "network/s3fanout.h"
#include "upload_gateway.h"
#include "util/atomic.h"
#include "util/pointer.h"

namespace upload {

class GatewayS3Uploader : public GatewayUploader {
 public:
  GatewayS3Uploader(const SpoolerDefinition &spooler_definition,
                    const std::string &s3_config_path,
                    const std::string &repo_alias);
  virtual ~GatewayS3Uploader();

  virtual std::string name() const { return "GatewayS3"; }

  virtual bool Initialize();

  virtual void WaitForUpload() const;

  virtual unsigned int GetNumberOfErrors() const;

 protected:
  virtual void FinalizeStreamedUpload(UploadStreamHandle *handle,
                                      const shash::Any &content_hash);

 private:
  bool InitS3Manager();

  static void *MainCollectResults(void *data);

  std::string s3_config_path_;
  std::string repo_alias_;
  UniquePtr<s3fanout::S3FanoutManager> s3fanout_mgr_;
  /// Written by the collect-results thread, read by the publisher thread
  mutable atomic_int32 s3_errors_;

  pthread_t thread_collect_results_;
  bool collector_running_;
};

}  // namespace upload

#endif  // CVMFS_UPLOAD_GATEWAY_S3_H_
