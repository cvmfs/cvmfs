/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_REPOSITORY_H_
#define CVMFS_PUBLISH_REPOSITORY_H_

#include <string>
#include <vector>

#include "history.h"  // for History::Tag
#include "publish/settings.h"
#include "upload_spooler_result.h"
#include "util/single_copy.h"

namespace catalog {
class DeltaCounters;
class DirectoryEntry;
class SimpleCatalogManager;
}
namespace download {
class DownloadManager;
}
namespace history {
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

enum RepositoryType {
  kClient,
  kPublisher,
  kReplica
};

/*
Repository* GetRepository(const std::string fqrn) {
  // here we only distinguish between a client and a publisher
  // we do not take care of replicas for now
  // should we call them Stratum0 and Straum1 instead of Publisher and Replica?
  //
  // In order to distinguish client and publisher we check if the spool directories exists
  // maybe there is a better way, but it will be good enouh for now
  //

  SettingsPublisher spub(fqrn);
  SettingsStorage stor = spub.storage();
}
*/

/**
 * Users create derived instances to react on repository diffs
 */
class __attribute__((visibility("default"))) DiffListener {
 public:
  virtual ~DiffListener() {}
  virtual void OnInit(const history::History::Tag &from_tag,
                      const history::History::Tag &to_tag) = 0;
  virtual void OnStats(const catalog::DeltaCounters &delta) = 0;
  virtual void OnAdd(const std::string &path,
                     const catalog::DirectoryEntry &entry) = 0;
  virtual void OnRemove(const std::string &path,
                        const catalog::DirectoryEntry &entry) = 0;
  virtual void OnModify(const std::string &path,
                        const catalog::DirectoryEntry &entry_from,
                        const catalog::DirectoryEntry &entry_to) = 0;
};

class __attribute__((visibility("default"))) Repository : SingleCopy {
 public:
  /**
   * Tag names beginning with @ are interpreted as raw hashes
   */
  static const char kRawHashSymbol = '@';

  static std::string GetFqrnFromUrl(const std::string &url);

  explicit Repository(const SettingsRepository &settings);
  virtual ~Repository();

  void Check();
  void GarbageCollect();
  void List();
  virtual const bool InTransaction() const { return false; };
  virtual const RepositoryType GetRepositoryType() const { return kClient; };

  /**
   * From and to are either tag names or catalog root hashes preceeded by
   * a '@'.
   */
  void Diff(const std::string &from, const std::string &to,
            DiffListener *diff_listener);

  /**
   * Checks whether the $url/.cvmfs_master_replica is available
   */
  bool IsMasterReplica();

  const signature::SignatureManager *signature_mgr() const {
    return signature_mgr_;
  }
  const upload::Spooler *spooler() const { return spooler_; }
  const whitelist::Whitelist *whitelist() const { return whitelist_; }
  const manifest::Manifest *manifest() const { return manifest_; }
  // Inheritance of History and SqliteHistory unknown in the header
  const history::History *history() const;
  std::string meta_info() const { return meta_info_; }

 protected:
  Repository();
  void DownloadRootObjects(
    const std::string &url,
    const std::string &fqrn,
    const std::string &tmp_dir);
  catalog::SimpleCatalogManager *GetSimpleCatalogManager();

  const SettingsRepository settings_;

  perf::Statistics *statistics_;
  signature::SignatureManager *signature_mgr_;
  download::DownloadManager *download_mgr_;
  /**
   * The read-only catalog manager, loaded on demand
   */
  catalog::SimpleCatalogManager *simple_catalog_mgr_;
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

  explicit Publisher(const SettingsPublisher &settings);
  virtual ~Publisher();

  void UpdateMetaInfo();
  void Transaction();
  void Abort();
  void Publish();
  void Ingest();
  void Sync();
  void Diff(DiffListener&);

  /**
   * Must not edit magic tags 'trunk' and 'trunk-previous'.
   * Removal of non-existing tags is silently ignored. The caller needs to
   * ensure that the data provided in new tags makes sense.
   */
  void EditTags(const std::vector<history::History::Tag> &add_tags,
                const std::vector<std::string> &rm_tags);
  /**
   * Create empty $url/.cvmfs_master_replica
   */
  void MarkReplicatible(bool value);

  void Rollback();
  void Resign();
  void Migrate();

  virtual const bool InTransaction() const override;

  const SettingsPublisher &settings() const { return settings_; }

  virtual const RepositoryType GetRepositoryType() const { return kPublisher; };

 private:
  Publisher();  ///< Used by Create

  void CreateKeychain();
  void CreateStorage();
  void CreateSpoolArea();
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

  void CheckTagName(const std::string &name);

  SettingsPublisher settings_;
  /**
   * The log level, set to kLogNone if settings_.is_silent() == true
   */
  int llvl_;
  bool in_transaction_;
};

class __attribute__((visibility("default"))) Replica : public Repository {
 public:
  static Replica *Create();
  explicit Replica(const SettingsReplica &settings);
  virtual ~Replica();

  virtual const RepositoryType GetRepositoryType() const { return kReplica; };

  void Snapshot();
};

}  // namespace publish

#endif  // CVMFS_PUBLISH_REPOSITORY_H_
