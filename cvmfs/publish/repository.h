/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_REPOSITORY_H_
#define CVMFS_PUBLISH_REPOSITORY_H_

#include <string>

#include "publish/settings.h"
#include "upload_spooler_result.h"
#include "util/single_copy.h"

namespace download {
class DownloadManager;
}
namespace history {
class History;
class SqliteHistory;
}
namespace manifest {
class Manifest;
class Reflog;
}
namespace perf {
class Statistics;
}
namespace signature {
class SignatureManager;
}
namespace upload {
class Spooler;
}
namespace whitelist {
class Whitelist;
}

namespace publish {

class Repository : SingleCopy {
 public:
  Repository();
  virtual ~Repository();

  void Check();
  void GarbageCollect();
  void List();
  void Diff();

  upload::Spooler *spooler() { return spooler_; }
  download::DownloadManager *download_mgr() { return download_mgr_; }
  whitelist::Whitelist *whitelist() { return whitelist_; }

  manifest::Manifest *manifest() { return manifest_; }
  // Inheritance of History and SqliteHisty unknown in the header
  history::History *history();

 protected:
  void DownloadRootObjects(
    const std::string &url,
    const std::string &fqrn,
    const std::string &tmp_dir);

  perf::Statistics *statistics_;
  signature::SignatureManager *signature_mgr_;
  download::DownloadManager *download_mgr_;
  upload::Spooler *spooler_;
  whitelist::Whitelist *whitelist_;
  manifest::Reflog *reflog_;
  manifest::Manifest *manifest_;
  history::SqliteHistory *history_;
  // TODO(jblomer): make MetaInfo class
  std::string meta_info_;
};

class __attribute__((visibility("default"))) Publisher : public Repository {
 public:
  static Publisher *Create(const SettingsPublisher &settings);

  Publisher(const SettingsPublisher &settings);
  virtual ~Publisher();

  void UpdateMetaInfo();
  void Transaction();
  void Abort();
  void Publish();
  void Ingest();
  void EditTags();
  void Rollback();
  void Resign();
  void Migrate();

  signature::SignatureManager *signature_mgr() { return signature_mgr_; }

 private:
  // Used by Create
  Publisher();

  void CreateKeychain();
  void CreateStorage();
  void CreateRootObjects();

  void ExportKeychain();
  void CreateDirectoryAsOwner(const std::string &path, int mode);
  void InitSpoolArea();

  void PushCertificate();
  void PushHistory();
  void PushManifest();
  void PushMetainfo();
  void PushReflog();
  void PushWhitelist();

  void OnProcessCertificate(const upload::SpoolerResult &result);
  void OnProcessHistory(const upload::SpoolerResult &result);
  void OnProcessMetainfo(const upload::SpoolerResult &result);
  void OnUploadManifest(const upload::SpoolerResult &result);
  void OnUploadReflog(const upload::SpoolerResult &result);
  void OnUploadWhitelist(const upload::SpoolerResult &result);

  SettingsPublisher settings_;
};

class Replica : public Repository {
 public:
  virtual ~Replica();
  static Replica *Create();
  void Snapshot();
};

}  // namespace publish

#endif  // CVMFS_PUBLISH_REPOSITORY_H_
