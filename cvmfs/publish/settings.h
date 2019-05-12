/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_SETTINGS_H_
#define CVMFS_PUBLISH_SETTINGS_H_

#include <stdint.h>
#include <unistd.h>

#include <string>

#include "compression.h"
#include "hash.h"
#include "sync_union.h"
#include "upload_spooler_definition.h"

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
  Setting(const T &v) : value_(v), is_default_(true) { }

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
};


// Settings from the point of construction always represent a valid
// configuration. The constructor sets default values, which can be overwritten
// by setters. The setters throw errors when invalid options are detected.

class SettingsSpoolArea {
public:
  explicit SettingsSpoolArea(const std::string &fqrn)
    : workspace_(std::string("/var/spool/cvmfs/") + fqrn)
    , tmp_dir_(workspace_() + "/tmp")
  { }

  void UseSystemTempDir();
  void SetSpoolArea(const std::string &path);

  std::string workspace() const { return workspace_; }
  std::string tmp_dir() const { return tmp_dir_; }
  std::string readonly_mnt() const { return workspace_() + "/rdonly"; }
  std::string union_mnt() const { return workspace_() + "/union"; }
  std::string scratch_dir() const { return workspace_() + "/scratch/current"; }
  std::string client_config() const { return workspace_() + "/client.config"; }
  std::string client_log() const { return workspace_() + "/usyslog.log"; }
  std::string cache_dir() const { return workspace_() + "/cache"; }
  std::string ovl_work_dir() const { return workspace_() + "/ovl_work"; }

private:
  Setting<std::string> workspace_;
  Setting<std::string> tmp_dir_;
};


class SettingsTransaction {
 public:
  explicit SettingsTransaction(const std::string &fqrn)
    : hash_algorithm_(shash::kSha1)
    , compression_algorithm_(zlib::kZlibDefault)
    , ttl_second_(240)
    , is_garbage_collectable_(true)
    , is_volatile_(false)
    , union_fs_(kUnionFsUnknown)
    , spool_area_(fqrn)
  {}

  void SetUnionFsType(const std::string &union_fs);
  void DetectUnionFsType();

  shash::Algorithms hash_algorithm() const { return hash_algorithm_; }
  zlib::Algorithms compression_algorithm() const {
    return compression_algorithm_;
  }
  uint32_t ttl_second() const { return ttl_second_; }
  bool is_garbage_collectable() const { return is_garbage_collectable_; }
  bool is_volatile() const { return is_volatile_; }
  std::string voms_authz() const { return voms_authz_; }

  const SettingsSpoolArea &spool_area() const { return spool_area_; }
  SettingsSpoolArea *GetSpoolArea() { return &spool_area_; }

 private:
  bool ValidateUnionFs();

  Setting<shash::Algorithms> hash_algorithm_;
  Setting<zlib::Algorithms> compression_algorithm_;
  Setting<uint32_t> ttl_second_;
  Setting<bool> is_garbage_collectable_;
  Setting<bool> is_volatile_;
  Setting<std::string> voms_authz_;
  Setting<UnionFsType> union_fs_;

  SettingsSpoolArea spool_area_;
};


class SettingsGc {
};


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
  void MakeS3(const std::string &s3_config, const std::string &tmp_dir);

  upload::SpoolerDefinition::DriverType type() const { return type_; }

 private:
  Setting<std::string> fqrn_;
  Setting<upload::SpoolerDefinition::DriverType> type_;
  Setting<std::string> tmp_dir_;
  Setting<std::string> endpoint_;
};

class SettingsKeychain {
 public:
  explicit SettingsKeychain(const std::string &fqrn)
    : fqrn_(fqrn)
    , master_private_key_path_(
        std::string("/etc/cvmfs/keys/") + fqrn + ".masterkey")
    , master_public_key_path_(std::string("/etc/cvmfs/keys/") + fqrn + ".pub")
    , private_key_path_(std::string("/etc/cvmfs/keys/") + fqrn + ".key")
    , certificate_path_(std::string("/etc/cvmfs/keys/") + fqrn + ".crt")
  {}

  void SetKeychainDir(const std::string &keychain_dir);

  bool HasDanglingMasterKeys() const;
  bool HasMasterKeys() const;
  bool HasDanglingRepositoryKeys() const;
  bool HasRepositoryKeys() const;

  std::string master_private_key_path() const {
    return master_private_key_path_;
  }
  std::string master_public_key_path() const { return master_public_key_path_; }
  std::string private_key_path() const { return private_key_path_; }
  std::string certificate_path() const { return certificate_path_; }

 private:
  Setting<std::string> fqrn_;
  Setting<std::string> master_private_key_path_;
  Setting<std::string> master_public_key_path_;
  Setting<std::string> private_key_path_;
  Setting<std::string> certificate_path_;
};

/**
 * Description of an editable repository.
 */
class SettingsPublisher {
 public:
  SettingsPublisher(const std::string &fqrn)
    : fqrn_(fqrn)
    , url_(std::string("http://localhost/cvmfs/") + fqrn)
    , owner_uid_(0)
    , owner_gid_(0)
    , whitelist_validity_days_(30)
    , storage_(fqrn_)
    , transaction_(fqrn_)
    , keychain_(fqrn_)
  { }

  void SetUrl(const std::string &url);
  void SetOwner(const std::string &user_name);
  void SetOwner(uid_t uid, gid_t gid);

  std::string fqrn() const { return fqrn_; }
  std::string url() const { return url_; }
  unsigned whitelist_validity_days() const { return whitelist_validity_days_; }
  uid_t owner_uid() const { return owner_uid_; }
  uid_t owner_gid() const { return owner_gid_; }

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

  SettingsGc gc_;
  SettingsStorage storage_;
  SettingsTransaction transaction_;
  SettingsKeychain keychain_;
};

/**
 * Description of a stratum 1
 */
class SettingsReplica {
 public:
  SettingsReplica(const std::string &fqrn)
    : fqrn_(fqrn)
    , alias_(fqrn)
    , url_(std::string("http://localhost/cvmfs/") + alias_())
  {}

 private:
  Setting<std::string> fqrn_;
  Setting<std::string> alias_;
  Setting<std::string> url_;
};

}  // namespace publish

#endif  // CVMFS_PUBLISH_SETTINGS_H_
