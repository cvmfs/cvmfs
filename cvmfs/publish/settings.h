/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_SETTINGS_H_
#define CVMFS_PUBLISH_SETTINGS_H_

#include <string>

namespace publish {

// Settings from the point of construction always represent a valid
// configuration. The constructor sets default values, which can be overwritten
// by setters. The setters throw errors when invalid options are detected.

class SettingsTransaction {
};

class SettingsGc {
};

class SettingsStorage {
 public:
  SettingsStorage(const std::string &fqrn)
    : type_("local")
    , tmp_dir_(std::string("local,/srv/cvmfs/") + fqrn + "/data/txn")
    , endpoint_(std::string("/srv/cvmfs/") + fqrn)
  { }

  void SetLocator(const std::string &locator);
  std::string GetLocator() const {
    return type_ + "," + tmp_dir_ + "," + endpoint_;
  }

 private:
  std::string type_;
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
  { }

 private:
  std::string fqrn_;
  std::string url_;
  std::string owner_;

  SettingsStorage storage_;
  SettingsGc gc_;
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
