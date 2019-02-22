/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_SETTINGS_H_
#define CVMFS_PUBLISH_SETTINGS_H_

#include <string>

#include "compression.h"
#include "hash.h"
#include "sync_union.h"
#include "upload_spooler_definition.h"

namespace publish {

// Settings from the point of construction always represent a valid
// configuration. The constructor sets default values, which can be overwritten
// by setters. The setters throw errors when invalid options are detected.

class SettingsSpoolArea {
public:
  explicit SettingsSpoolArea(const std::string &fqrn)
    : spool_area_(std::string("/var/spool/cvmfs/") + fqrn)
    , tmp_dir_(spool_area_ + "/tmp")
  { }

  std::string tmp_dir() const { return tmp_dir_; }

private:
  std::string spool_area_;
  std::string tmp_dir_;
};


class SettingsTransaction {
 public:
  explicit SettingsTransaction(const std::string &fqrn)
    : hash_algorithm_(shash::kSha1)
    , compression_algorithm_(zlib::kZlibDefault)
    , union_fs_(kUnionFsUnknown)
    , spool_area_(fqrn)
  {}

  void SetUnionFsType(const std::string &union_fs);
  void DetectUnionFsType();

  shash::Algorithms hash_algorithm() const { return hash_algorithm_; }
  zlib::Algorithms compression_algorithm() const {
    return compression_algorithm_;
  }

  const SettingsSpoolArea &spool_area() const { return spool_area_; }
  SettingsSpoolArea *GetSpoolArea() { return &spool_area_; }

 private:
  bool ValidateUnionFs();

  shash::Algorithms hash_algorithm_;
  zlib::Algorithms compression_algorithm_;
  UnionFsType union_fs_;

  SettingsSpoolArea spool_area_;
};


class SettingsGc {
};


class SettingsStorage {
 public:
  explicit SettingsStorage(const std::string &fqrn)
    : fqrn_(fqrn)
    , type_(upload::SpoolerDefinition::Local)
    , tmp_dir_(std::string("/srv/cvmfs/") + fqrn_ + "/data/txn")
    , endpoint_(std::string("/srv/cvmfs/") + fqrn_)
  { }

  std::string GetLocator() const;
  void SetLocator(const std::string &locator);
  void MakeS3(const std::string &s3_config,
              const SettingsSpoolArea &spool_area);

  upload::SpoolerDefinition::DriverType type() const { return type_; }

 private:
  std::string fqrn_;
  upload::SpoolerDefinition::DriverType type_;
  std::string tmp_dir_;
  std::string endpoint_;
};

/**
 * Description of an editable repository.
 */
class SettingsPublisher {
 public:
  SettingsPublisher(const std::string &fqrn)
    : fqrn_(fqrn)
    , url_(std::string("http://localhost/cvmfs/") + fqrn_)
    , storage_(fqrn_)
    , transaction_(fqrn_)
  { }

  void SetUrl(const std::string &url);

  const SettingsStorage &storage() const { return storage_; }
  const SettingsTransaction &transaction() const { return transaction_; }
  SettingsStorage *GetStorage() { return &storage_; }
  SettingsTransaction *GetTransaction() { return &transaction_; }

 private:
  std::string fqrn_;
  std::string url_;
  std::string owner_;

  SettingsGc gc_;
  SettingsStorage storage_;
  SettingsTransaction transaction_;
};

/**
 * Description of a stratum 1
 */
class SettingsReplica {
 public:
  SettingsReplica(const std::string &fqrn)
    : fqrn_(fqrn)
    , alias_(fqrn_)
    , url_(std::string("http://localhost/cvmfs/") + alias_)
  {}

 private:
  std::string fqrn_;
  std::string alias_;
  std::string url_;
};

}  // namespace publish

#endif  // CVMFS_PUBLISH_SETTINGS_H_
