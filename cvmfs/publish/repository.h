/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_REPOSITORY_H_
#define CVMFS_PUBLISH_REPOSITORY_H_

#include <string>
#include <vector>

#include "gateway_util.h"
#include "history.h"  // for History::Tag
#include "publish/settings.h"
#include "upload_spooler_result.h"
#include "util/pointer.h"
#include "util/single_copy.h"

namespace catalog {
class DeltaCounters;
class DirectoryEntry;
class SimpleCatalogManager;
class WritableCatalogManager;
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
class StatisticsTemplate;
}
namespace signature {
class SignatureManager;
}
class SyncMediator;
class SyncParameters;
namespace upload {
class Spooler;
}
namespace whitelist {
class Whitelist;
}

namespace publish {

class SyncUnion;

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


class __attribute__((visibility("default"))) Env {
 public:
  /**
   * Depending on the desired course of action, the permitted capabilites of the
   * binary (cap_dac_read_search, cap_sys_admin) needs to be dropped or gained.
   * Dropped for creating user namespaces in `enter`, gained for walking through
   * overlayfs.
   */
  static void DropCapabilities();

  /**
   * If in an ephemeral writable shell, return the session directory.
   * Otherwise return the empty string.
   */
  static std::string GetEnterSessionDir();
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
  whitelist::Whitelist *whitelist_;
  manifest::Reflog *reflog_;
  manifest::Manifest *manifest_;
  history::SqliteHistory *history_;
  // TODO(jblomer): make MetaInfo class
  std::string meta_info_;
};

class __attribute__((visibility("default"))) Publisher : public Repository {
 public:
  /**
   * Encapsulates operations on a dedicated publisher
   */
  class ManagedNode {
   public:
    /**
     * Collection of publisher failure states (see Check())
     */
    enum EFailures {
      kFailOk                   = 0,
      kFailRdOnlyBroken         = 0x01,
      kFailRdOnlyOutdated       = 0x02,
      kFailRdOnlyWrongRevision  = 0x04,
      kFailUnionBroken          = 0x08,
      kFailUnionWritable        = 0x10,
      kFailUnionLocked          = 0x20,
    };

    explicit ManagedNode(Publisher *p) : publisher_(p) {}
    /**
     * Verifies the mountpoints and the transaction status. Returns a bit map
     * of EFailures codes.
     */
    int Check(bool is_quiet = false);
    /**
     * Re-mount /cvmfs/$fqrn read-writable
     */
    void Open();
    /**
     * Re-mount /cvmfs/$fqrn read-only
     */
    void Lock();

   private:
    /**
     * Possible state transitions for the cvmfs read-only mountpoint and the
     * union file system on /cvmfs/$fqrn
     */
    enum EMountpointAlterations {
      kAlterUnionUnmount,
      kAlterRdOnlyUnmount,
      kAlterUnionMount,
      kAlterRdOnlyMount,
      kAlterUnionOpen,
      kAlterUnionLock,
    };

    void AlterMountpoint(EMountpointAlterations how, int log_level);
    void SetRootHash(const shash::Any &hash);

    Publisher *publisher_;
  };  // class ManagedNode


  /**
   * A session encapsulates an active storage (gateway) lease
   */
  class Session : ::SingleCopy {
   public:
    struct Settings {
      Settings() : llvl(0) {}
      std::string service_endpoint;
      /**
       * $fqrn/$lease_path
       */
      std::string repo_path;
      std::string gw_key_path;
      std::string token_path;
      int llvl;
    };

    static Session *Create(const Settings &settings_session);
    static Session *Create(const SettingsPublisher &settings_publisher,
                           int llvl = 0);
    ~Session();
   private:
    explicit Session(const Settings &settings_session);
    void Acquire();
    Settings settings_;
  };  // class Session

  /**
   * The directory layout of the publisher node must be of matching revision
   */
  static const unsigned kRequiredLayoutRevision = 142;

  static Publisher *Create(const SettingsPublisher &settings);

  explicit Publisher(const SettingsPublisher &settings);
  virtual ~Publisher();

  void UpdateMetaInfo();
  void Transaction() { TransactionRetry(); }
  void Abort();
  void Publish();
  void Ingest();
  void Sync();

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

  const SettingsPublisher &settings() const { return settings_; }
  bool in_transaction() const { return in_transaction_; }
  ManagedNode *managed_node() const { return managed_node_.weak_ref(); }
  const upload::Spooler *spooler_files() const { return spooler_files_; }
  const upload::Spooler *spooler_catalogs() const { return spooler_catalogs_; }

 private:
  Publisher();  ///< Used by Create

  /**
   * Used just before a spooler is required, e.g. in Create()
   */
  void ConstructSpoolers();
  /**
   * Initializes the spooler, the writable catalog manager, and the sync
   * mediator
   */
  void ConstructSyncManagers();
  void WipeScratchArea();

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

  void TransactionRetry();
  void TransactionImpl();
  void CheckTransactionStatus();

  SettingsPublisher settings_;
  UniquePtr<perf::StatisticsTemplate> statistics_publish_;
  /**
   * The log level, set to kLogNone if settings_.is_silent() == true
   */
  int llvl_;
  bool in_transaction_;
  gateway::GatewayKey gw_key_;
  UniquePtr<ManagedNode> managed_node_;

  upload::Spooler *spooler_files_;
  upload::Spooler *spooler_catalogs_;
  catalog::WritableCatalogManager *catalog_mgr_;
  SyncParameters *sync_parameters_;
  SyncMediator *sync_mediator_;
  publish::SyncUnion *sync_union_;
};

class __attribute__((visibility("default"))) Replica : public Repository {
 public:
  static Replica *Create();
  explicit Replica(const SettingsReplica &settings);
  virtual ~Replica();

  void Snapshot();
};

}  // namespace publish

#endif  // CVMFS_PUBLISH_REPOSITORY_H_
