/**
 * This file is part of the CernVM File System
 */

#ifndef CVMFS_SIGNING_TOOL_H_
#define CVMFS_SIGNING_TOOL_H_

#include <string>
#include <vector>

#include "crypto/hash.h"
#include "server_tool.h"
#include "util/future.h"

class ServerTool;

namespace upload {
struct SpoolerResult;
}

class SigningTool {
 public:
  enum Result {
    kSuccess,
    kError,
    kInitError,
    kReflogMissing,
    kReflogChecksumMissing
  };

  explicit SigningTool(ServerTool *server_tool);
  virtual ~SigningTool();

  Result Run(const std::string &manifest_path, const std::string &repo_url,
             const std::string &spooler_definition, const std::string &temp_dir,
             const std::string &certificate = "",
             const std::string &priv_key = "",
             const std::string &repo_name = "", const std::string &pwd = "",
             const std::string &meta_info = "",
             const std::string &reflog_chksum_path = "",
             const std::string &proxy = "",
             const bool garbage_collectable = false,
             const bool bootstrap_shortcuts = false,
             const bool return_early = false,
             const std::vector<shash::Any> reflog_catalogs =
                 std::vector<shash::Any>());

 protected:
  void CertificateUploadCallback(const upload::SpoolerResult &result);
  void MetainfoUploadCallback(const upload::SpoolerResult &result);

 private:
  ServerTool *server_tool_;
  Future<shash::Any> certificate_hash_;
  Future<shash::Any> metainfo_hash_;
};

#endif  // CVMFS_SIGNING_TOOL_H_
