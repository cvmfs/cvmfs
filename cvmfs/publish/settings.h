/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_SETTINGS_H_
#define CVMFS_PUBLISH_SETTINGS_H_

#include <stdint.h>
#include <unistd.h>

#include <map>
#include <string>

#include "compression.h"
#include "hash.h"
#include "sync_union.h"
#include "upload_spooler_definition.h"

class OptionsManager;

namespace publish {

/**
 * Allows for settings that remember whether they have been explictily
 * overwritten.  Otherwise, default values can be changed to upstream repository
 * settings.
 */
template <class T>
class Setting {
 public:
  Setting() : value_(), is_default_(true) { }
  explicit Setting(const T &v) : value_(v), is_default_(true) { }

  Setting& operator=(const T &v) {
    value_ = v;
    is_default_ = false;
    return *this;
  }

  operator const T& () const {
    return value_;
  }

  const T& operator()() const {
    return value_;
  }

  bool SetIfDefault(const T &v) {
    if (!is_default_) return false;
    value_ = v;
    is_default_ = false;
    return true;
  }

  bool is_default() const { return is_default_; }

  T* GetPtr() { return &value_; }

 private:
  T value_;
  bool is_default_;
};  // Setting


/**
 * Steers the aggressiveness of Publisher::ManagedNode::Check()
 */
enum EUnionMountRepairMode {
  kUnionMountRepairNever = 0,
  kUnionMountRepairSafe,
  kUnionMountRepairAlways
};


// Settings from the point of construction always represent a valid
// configuration. The constructor sets default values, which can be overwritten
// by setters. The setters throw errors when invalid options are detected.

class SettingsSpoolArea {
 public:
  explicit SettingsSpoolArea(const std::string &fqrn)
    : workspace_(std::string("/var/spool/cvmfs/") + fqrn)
    , tmp_dir_(workspace_() + "/tmp")
    , union_mnt_(std::string("/cvmfs/") + fqrn)
    , repair_mode_(kUnionMountRepairSafe)
  { }

  void UseSystemTempDir();
  void SetSpoolArea(const std::string &path);
  void SetUnionMount(const std::string &path);
  void SetRepairMode(const EUnionMountRepairMode val);

  // Creates, if necessary, all the directories in the spool area and the temp
  // directory.  Does not take care of the union mount point.
  void EnsureDirectories();

  std::string workspace() const { return workspace_; }
  std::string tmp_dir() const { return tmp_dir_; }
  std::string readonly_mnt() const { return workspace_() + "/rdonly"; }
  std::string readonly_talk_socket() const {
     return workspace_() + "/cvmfs_io";
  }
  std::string union_mnt() const { return union_mnt_; }
  std::string scratch_base() const { return workspace_() + "/scratch"; }
  std::string scratch_dir() const { return scratch_base() + "/current"; }
  std::string scratch_wastebin() const { return scratch_base() + "/wastebin"; }
  std::string log_dir() const { return workspace() + "/logs"; }
  // TODO(jblomer): shouldn't this be in /etc/cvmfs/repositor.../client.conf
  std::string client_config() const { return workspace_() + "/client.config"; }
  std::string client_lconfig() const { return workspace_() + "/client.local"; }
  std::string client_log() const { return log_dir() + "/cvmfs.log"; }
  std::string cache_dir() const { return workspace_() + "/cache"; }
  std::string ovl_work_dir() const { return workspace_() + "/ovl_work"; }
  std::string checkout_marker() const { return workspace_() + "/checkout"; }
  std::string gw_session_token() const {
    return workspace_() + "/session_token";
  }
  std::string transaction_lock() const {
    return workspace_() + "/in_transaction.lock";
  }
  std::string publishing_lock() const {
    return workspace_() + "/is_publishing.lock";
  }
  EUnionMountRepairMode repair_mode() const { return repair_mode_; }

 private:
  Setting<std::string> workspace_;
  Setting<std::string> tmp_dir_;
  Setting<std::string> union_mnt_;
  /**
   * How aggresively should the mount point stack be repaired
   */
  Setting<EUnionMountRepairMode> repair_mode_;
};  // SettingsSpoolArea


class SettingsTransaction {
 public:
  explicit SettingsTransaction(const std::string &fqrn)
    : layout_revision_(0)
    , in_enter_session_(false)
    , hash_algorithm_(shash::kShake128)
    , compression_algorithm_(zlib::kZlibDefault)
    , ttl_second_(240)
    , is_garbage_collectable_(true)
    , is_volatile_(false)
    , enforce_limits_(false)
    // SyncParameters::kDefaultNestedKcatalogLimit
    , limit_nested_catalog_kentries_(500)
    // SyncParameters::kDefaultRootKcatalogLimit
    , limit_root_catalog_kentries_(500)
    // SyncParameters::kDefaultFileMbyteLimit
    , limit_file_size_mb_(1024)
    , use_catalog_autobalance_(false)
    // SyncParameters::kDefaultMaxWeight
    , autobalance_max_weight_(100000)
    // SyncParameters::kDefaultMinWeight
    , autobalance_min_weight_(1000)
    , print_changeset_(false)
    , dry_run_(false)
    , union_fs_(kUnionFsUnknown)
    , timeout_s_(0)
    , spool_area_(fqrn)
  {}

  void SetLayoutRevision(const unsigned revision);
  void SetInEnterSession(const bool value);
  void SetBaseHash(const shash::Any &hash);
  void SetUnionFsType(const std::string &union_fs);
  void SetHashAlgorithm(const std::string &algorithm);
  void SetCompressionAlgorithm(const std::string &algorithm);
  void SetEnforceLimits(bool value);
  void SetLimitNestedCatalogKentries(unsigned value);
  void SetLimitRootCatalogKentries(unsigned value);
  void SetLimitFileSizeMb(unsigned value);
  void SetUseCatalogAutobalance(bool value);
  void SetAutobalanceMaxWeight(unsigned value);
  void SetAutobalanceMinWeight(unsigned value);
  void SetPrintChangeset(bool value);
  void SetDryRun(bool value);
  void SetTimeout(unsigned seconds);
  void SetLeasePath(const std::string &path);
  void SetTemplate(const std::string &from, const std::string &to);
  void DetectUnionFsType();

  /**
   * 0 - wait infinitely
   * <0: unset, fail immediately
   */
  int GetTimeoutS() const;

  unsigned layout_revision() const { return layout_revision_; }
  bool in_enter_session() const { return in_enter_session_; }
  shash::Any base_hash() const { return base_hash_; }
  shash::Algorithms hash_algorithm() const { return hash_algorithm_; }
  zlib::Algorithms compression_algorithm() const {
    return compression_algorithm_;
  }
  uint32_t ttl_second() const { return ttl_second_; }
  bool is_garbage_collectable() const { return is_garbage_collectable_; }
  bool is_volatile() const { return is_volatile_; }
  bool enforce_limits() const { return enforce_limits_; }
  unsigned limit_nested_catalog_kentries() const {
    return limit_nested_catalog_kentries_;
  }
  unsigned limit_root_catalog_kentries() const {
    return limit_root_catalog_kentries_;
  }
  unsigned limit_file_size_mb() const { return limit_file_size_mb_; }
  bool use_catalog_autobalance() const { return use_catalog_autobalance_; }
  unsigned autobalance_max_weight() const { return autobalance_max_weight_; }
  unsigned autobalance_min_weight() const { return autobalance_min_weight_; }
  bool print_changeset() const { return print_changeset_; }
  bool dry_run() const { return dry_run_; }
  std::string voms_authz() const { return voms_authz_; }
  UnionFsType union_fs() const { return union_fs_; }
  std::string lease_path() const { return lease_path_; }
  std::string template_from() const { return template_from_; }
  std::string template_to() const { return template_to_; }

  const SettingsSpoolArea &spool_area() const { return spool_area_; }
  SettingsSpoolArea *GetSpoolArea() { return &spool_area_; }

  bool HasTemplate() const { return !template_to().empty(); }

 private:
  bool ValidateUnionFs();

  /**
   * See CVMFS_CREATOR_VERSION
   */
  Setting<unsigned> layout_revision_;
  /**
   * Set to true if the settings have been created from the environment of
   * the ephemeral writable shell (cvmfs_server enter command).
   */
  Setting<bool> in_enter_session_;
  /**
   * The root catalog hash based on which the transaction takes place.
   * Usually the current root catalog from the manifest, which should be equal
   * to the root hash of the mounted read-only volume.  In some cases, this
   * can be different though, e.g. for checked out branches or after silent
   * transactions such as template transactions.
   */
  Setting<shash::Any> base_hash_;
  Setting<shash::Algorithms> hash_algorithm_;
  Setting<zlib::Algorithms> compression_algorithm_;
  Setting<uint32_t> ttl_second_;
  Setting<bool> is_garbage_collectable_;
  Setting<bool> is_volatile_;
  Setting<bool> enforce_limits_;
  Setting<unsigned> limit_nested_catalog_kentries_;
  Setting<unsigned> limit_root_catalog_kentries_;
  Setting<unsigned> limit_file_size_mb_;
  Setting<bool> use_catalog_autobalance_;
  Setting<unsigned> autobalance_max_weight_;
  Setting<unsigned> autobalance_min_weight_;
  Setting<bool> print_changeset_;
  Setting<bool> dry_run_;
  Setting<std::string> voms_authz_;
  Setting<UnionFsType> union_fs_;
  /**
   * How long to retry taking a lease before giving up
   */
  Setting<unsigned> timeout_s_;
  Setting<std::string> lease_path_;
  /**
   * Used for template transactions where a directory tree gets cloned
   * (from --> to) as part of opening the transaction
   */
  Setting<std::string> template_from_;
  Setting<std::string> template_to_;

  SettingsSpoolArea spool_area_;
};  // class SettingsTransaction


class SettingsGc {
};  // class SettingsGc


class SettingsStorage {
 public:
  explicit SettingsStorage(const std::string &fqrn)
    : fqrn_(fqrn)
    , type_(upload::SpoolerDefinition::Local)
    , tmp_dir_(std::string("/srv/cvmfs/") + fqrn + "/data/txn")
    , endpoint_(std::string("/srv/cvmfs/") + fqrn)
  { }

  std::string GetLocator() const;
  void SetLocator(const std::string &locator);
  void MakeLocal(const std::string &path);
  void MakeS3(const std::string &s3_config, const std::string &tmp_dir);
  void MakeGateway(const std::string &host, unsigned port,
                   const std::string &tmp_dir);

  upload::SpoolerDefinition::DriverType type() const { return type_; }
  std::string endpoint() const { return endpoint_; }

 private:
  Setting<std::string> fqrn_;
  Setting<upload::SpoolerDefinition::DriverType> type_;
  Setting<std::string> tmp_dir_;
  Setting<std::string> endpoint_;
};  // class SettingsStorage


class SettingsKeychain {
 public:
  explicit SettingsKeychain(const std::string &fqrn)
    : fqrn_(fqrn)
    , keychain_dir_("/etc/cvmfs/keys")
    , master_private_key_path_(keychain_dir_() + "/" + fqrn + ".masterkey")
    , master_public_key_path_(keychain_dir_() + "/" + fqrn + ".pub")
    , private_key_path_(keychain_dir_() + "/" + fqrn + ".key")
    , certificate_path_(keychain_dir_() + "/" + fqrn + ".crt")
    , gw_key_path_(keychain_dir_() + "/" + fqrn + ".gw")
  {}

  void SetKeychainDir(const std::string &keychain_dir);

  bool HasDanglingMasterKeys() const;
  bool HasMasterKeys() const;
  bool HasDanglingRepositoryKeys() const;
  bool HasRepositoryKeys() const;
  bool HasGatewayKey() const;

  std::string keychain_dir() const { return keychain_dir_; }
  std::string master_private_key_path() const {
    return master_private_key_path_;
  }
  std::string master_public_key_path() const { return master_public_key_path_; }
  std::string private_key_path() const { return private_key_path_; }
  std::string certificate_path() const { return certificate_path_; }
  std::string gw_key_path() const { return gw_key_path_; }

 private:
  Setting<std::string> fqrn_;
  Setting<std::string> keychain_dir_;
  Setting<std::string> master_private_key_path_;
  Setting<std::string> master_public_key_path_;
  Setting<std::string> private_key_path_;
  Setting<std::string> certificate_path_;
  Setting<std::string> gw_key_path_;
};  // class SettingsKeychain


class SettingsPublisher;

/**
 * Description of a read-only repository
 */
class SettingsRepository {
 public:
  explicit SettingsRepository(const std::string &fqrn)
    : fqrn_(fqrn)
    , url_(std::string("http://localhost/cvmfs/") + fqrn_())
    , tmp_dir_("/tmp")
    , keychain_(fqrn)
  {}
  explicit SettingsRepository(const SettingsPublisher &settings_publisher);

  void SetUrl(const std::string &url);
  void SetTmpDir(const std::string &tmp_dir);

  std::string fqrn() const { return fqrn_; }
  std::string url() const { return url_; }
  std::string tmp_dir() const { return tmp_dir_; }

  const SettingsKeychain &keychain() const { return keychain_; }
  SettingsKeychain *GetKeychain() { return &keychain_; }

 private:
  Setting<std::string> fqrn_;
  Setting<std::string> url_;
  Setting<std::string> tmp_dir_;

  SettingsKeychain keychain_;
};  // class SettingsRepository


/**
 * Description of an editable repository.
 */
class SettingsPublisher {
 public:
  static const unsigned kDefaultWhitelistValidity;  // 30 days

  explicit SettingsPublisher(const std::string &fqrn)
    : fqrn_(fqrn)
    , url_(std::string("http://localhost/cvmfs/") + fqrn)
    , owner_uid_(0)
    , owner_gid_(0)
    , whitelist_validity_days_(kDefaultWhitelistValidity)
    , is_silent_(false)
    , is_managed_(false)
    , storage_(fqrn_)
    , transaction_(fqrn_)
    , keychain_(fqrn_)
  { }
  explicit SettingsPublisher(const SettingsRepository &settings_repository);

  void SetUrl(const std::string &url);
  void SetOwner(const std::string &user_name);
  void SetOwner(uid_t uid, gid_t gid);
  void SetIsSilent(bool value);
  void SetIsManaged(bool value);

  std::string GetReadOnlyXAttr(const std::string &attr);

  std::string fqrn() const { return fqrn_; }
  std::string url() const { return url_; }
  unsigned whitelist_validity_days() const { return whitelist_validity_days_; }
  uid_t owner_uid() const { return owner_uid_; }
  uid_t owner_gid() const { return owner_gid_; }
  bool is_silent() const { return is_silent_; }
  bool is_managed() const { return is_managed_; }

  const SettingsStorage &storage() const { return storage_; }
  const SettingsTransaction &transaction() const { return transaction_; }
  const SettingsKeychain &keychain() const { return keychain_; }
  SettingsStorage *GetStorage() { return &storage_; }
  SettingsTransaction *GetTransaction() { return &transaction_; }
  SettingsKeychain *GetKeychain() { return &keychain_; }

 private:
  Setting<std::string> fqrn_;
  Setting<std::string> url_;
  Setting<uid_t> owner_uid_;
  Setting<gid_t> owner_gid_;
  Setting<unsigned> whitelist_validity_days_;
  Setting<bool> is_silent_;
  Setting<bool> is_managed_;

  SettingsStorage storage_;
  SettingsTransaction transaction_;
  SettingsKeychain keychain_;
};  // SettingsPublisher


/**
 * Description of a stratum 1
 */
class SettingsReplica {
 public:
  explicit SettingsReplica(const std::string &fqrn)
    : fqrn_(fqrn)
    , alias_(fqrn)
    , url_(std::string("http://localhost/cvmfs/") + alias_())
  {}

 private:
  Setting<std::string> fqrn_;
  Setting<std::string> alias_;
  Setting<std::string> url_;
};  // class SettingsReplica


/**
 * Create Settings objects from the system configuration in
 * /etc/cvmfs/repositories.d
 */
class SettingsBuilder : SingleCopy {
 public:
  SettingsBuilder()
    : config_path_("/etc/cvmfs/repositories.d")
    , options_mgr_(NULL)
  {}
  ~SettingsBuilder();
  /**
   * Used in unit tests.
   */
  explicit SettingsBuilder(const std::string c) : config_path_(c) {}

  /**
   * If ident is a url, creates a generic settings object inferring the fqrn
   * from the url.
   * Otherwise, looks in the config files in /etc/cvmfs/repositories.d/<alias>/
   * If alias is an empty string, the command still succeds iff there is a
   * single repository under /etc/cvmfs/repositories.d
   */
  SettingsRepository CreateSettingsRepository(const std::string &ident);

  /**
   * If ident is a url, creates a generic settings object inferring the fqrn
   * from the url.
   * Otherwise, looks in the config files in /etc/cvmfs/repositories.d/<alias>/
   * If alias is an empty string, the command still succeds iff there is a
   * single repository under /etc/cvmfs/repositories.d
   * If needs_managed is true, remote repositories are rejected
   * In an "enter environment" (see cmd_enter), the spool area of the enter
   * environment is applied.
   */

  SettingsPublisher* CreateSettingsPublisher(
      const std::string &ident, bool needs_managed = false);

  OptionsManager *options_mgr() const { return options_mgr_; }
  bool IsManagedRepository() const { return options_mgr_ != NULL; }

 private:
  std::string config_path_;
  /**
   * For locally managed repositories, the options manager is non NULL and
   * contains the configuration after a call to CreateSettingsRepository()
   */
  OptionsManager *options_mgr_;

  /**
   * Returns the name of the one and only repository under kConfigPath
   * Throws an exception if there are none or multiple repositories.
   * The alias is usually the fqrn except for a replica with an explicit
   * alias set different from the fqrn (e.g. if Stratum 0 and 1 are hosted)
   * on the same node.
   */
  std::string GetSingleAlias();

  /**
   * If in a ephemeral writable shell, parse $session_dir/env.conf
   * Otherwise return an empty map. A non-empty map has at least CVMFS_FQRN set.
   */
  std::map<std::string, std::string> GetSessionEnvironment();

  /**
   * Create settings from an ephermal writable shell
   */
  SettingsPublisher* CreateSettingsPublisherFromSession();
};  // class SettingsBuilder

}  // namespace publish

#endif  // CVMFS_PUBLISH_SETTINGS_H_
