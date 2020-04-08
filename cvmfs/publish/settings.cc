/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "publish/settings.h"

#include <cstdlib>
#include <string>
#include <vector>

#include "options.h"
#include "publish/except.h"
#include "publish/repository.h"
#include "sanitizer.h"
#include "util/posix.h"
#include "util/string.h"

namespace publish {

void SettingsSpoolArea::UseSystemTempDir() {
  if (getenv("TMPDIR") != NULL)
    tmp_dir_ = getenv("TMPDIR");
  else
    tmp_dir_ = "/tmp";
}

void SettingsSpoolArea::SetSpoolArea(const std::string &path) {
  workspace_ = path;
  tmp_dir_ = workspace_() + "/tmp";
}


//------------------------------------------------------------------------------


void SettingsTransaction::SetUnionFsType(const std::string &union_fs) {
  if (union_fs == "aufs") {
    union_fs_ = kUnionFsAufs;
  } else if ((union_fs == "overlay") || (union_fs == "overlayfs")) {
    union_fs_ = kUnionFsOverlay;
  } else if (union_fs == "tarball") {
    union_fs_ = kUnionFsTarball;
  } else {
    throw EPublish("unsupported union file system: " + union_fs);
  }
}

void SettingsTransaction::DetectUnionFsType() {
  // TODO(jblomer): shall we switch the order?
  if (DirectoryExists("/sys/fs/aufs")) {
    union_fs_ = kUnionFsAufs;
    return;
  }
  // TODO(jblomer): modprobe aufs, try again
  if (DirectoryExists("/sys/module/overlay")) {
    union_fs_ = kUnionFsOverlay;
    return;
  }
  // TODO(jblomer): modprobe overlay, try again
  throw EPublish("neither AUFS nor OverlayFS detected on the system");
}

bool SettingsTransaction::ValidateUnionFs() {
  // TODO(jblomer)
  return true;
}

//------------------------------------------------------------------------------


std::string SettingsStorage::GetLocator() const {
  return std::string(upload::SpoolerDefinition::kDriverNames[type_]) +
    "," + tmp_dir_() +
    "," + endpoint_();
}

void SettingsStorage::MakeS3(
  const std::string &s3_config,
  const std::string &tmp_dir)
{
  type_ = upload::SpoolerDefinition::S3;
  tmp_dir_ = tmp_dir;
  endpoint_ = "cvmfs/" + fqrn_() + "@" + s3_config;
}

void SettingsStorage::MakeLocal(const std::string &path) {
  type_ = upload::SpoolerDefinition::Local;
  endpoint_ = path;
  tmp_dir_ = path + "/data/txn";
}

void SettingsStorage::SetLocator(const std::string &locator) {
  std::vector<std::string> tokens = SplitString(locator, ',');
  if (tokens.size() != 3) {
    throw EPublish("malformed storage locator, expected format is "
                   "<type>,<temporary directory>,<endpoint>");
  }
  if (tokens[0] == "local") {
    type_ = upload::SpoolerDefinition::Local;
  } else if (tokens[0] == "S3") {
    type_ = upload::SpoolerDefinition::S3;
  } else if (tokens[0] == "gw") {
    type_ = upload::SpoolerDefinition::Gateway;
  } else {
    throw EPublish("unsupported storage type: " + tokens[0]);
  }
  tmp_dir_ = tokens[1];
  endpoint_ = tokens[2];
}


//------------------------------------------------------------------------------

void SettingsKeychain::SetKeychainDir(const std::string &keychain_dir) {
  keychain_dir_ = keychain_dir;
  master_private_key_path_ = keychain_dir + "/" + fqrn_() + ".masterkey";
  master_public_key_path_ = keychain_dir + "/" + fqrn_() + ".pub";
  private_key_path_ = keychain_dir + "/" + fqrn_() + ".key";
  certificate_path_ = keychain_dir + "/" + fqrn_() + ".crt";
}


bool SettingsKeychain::HasDanglingMasterKeys() const {
  return (FileExists(master_private_key_path_) &&
          !FileExists(master_public_key_path_)) ||
         (!FileExists(master_private_key_path_) &&
          FileExists(master_public_key_path_));
}


bool SettingsKeychain::HasMasterKeys() const {
  return FileExists(master_private_key_path_) &&
         FileExists(master_public_key_path_);
}


bool SettingsKeychain::HasDanglingRepositoryKeys() const {
  return (FileExists(private_key_path_) &&
          !FileExists(certificate_path_)) ||
         (!FileExists(private_key_path_) &&
          FileExists(certificate_path_));
}


bool SettingsKeychain::HasRepositoryKeys() const {
  return FileExists(private_key_path_) &&
         FileExists(certificate_path_);
}

//------------------------------------------------------------------------------


SettingsRepository::SettingsRepository(
  const SettingsPublisher &settings_publisher)
  : fqrn_(settings_publisher.fqrn())
  , url_(settings_publisher.url())
  , tmp_dir_(settings_publisher.transaction().spool_area().tmp_dir())
  , keychain_(settings_publisher.fqrn())
{
  keychain_.SetKeychainDir(settings_publisher.keychain().keychain_dir());
}


void SettingsRepository::SetUrl(const std::string &url) {
  // TODO(jblomer): sanitiation, check availability
  url_ = url;
}


void SettingsRepository::SetTmpDir(const std::string &tmp_dir) {
  tmp_dir_ = tmp_dir;
}


//------------------------------------------------------------------------------
/**
 * We check if the server.conf configuration file is there
 * More sophistaced checks should be put in place
 */
bool SettingsPublisher::CanBe(const std::string &fqrn) {
  try {
    const std::string reponame = GetFQRN(fqrn);
    const std::string server_conf =
        kConfigPath + "/" + reponame + "/" + "server.conf";
    printf("Checking if exists: %s", server_conf.c_str());
    return FileExists(server_conf);
  } catch (...) {
    return false;
  }
}

void SettingsPublisher::SetUrl(const std::string &url) {
  // TODO(jblomer): sanitiation, check availability
  url_ = url;
}


void SettingsPublisher::SetOwner(const std::string &user_name) {
  bool retval = GetUidOf(user_name, owner_uid_.GetPtr(), owner_gid_.GetPtr());
  if (!retval) {
    throw EPublish("unknown user name for repository owner");
  }
}

void SettingsPublisher::SetOwner(uid_t uid, gid_t gid) {
  owner_uid_ = uid;
  owner_gid_ = gid;
}

void SettingsPublisher::SetIsSilent(bool value) {
  is_silent_ = value;
}


//------------------------------------------------------------------------------


SettingsBuilder::~SettingsBuilder() {
  delete options_mgr_;
}


const std::string GetSingleAlias() {
  std::vector<std::string> repositories = FindDirectories(kConfigPath);
  if (repositories.empty())
    throw EPublish("no repositories available in " + kConfigPath);
  if (repositories.size() > 1)
    throw EPublish("multiple repositories available in " + kConfigPath);
  return repositories[0];
}


SettingsRepository SettingsBuilder::CreateSettingsRepository(
  const std::string &ident)
{
  if (HasPrefix(ident, "http://", true /* ignore case */) ||
      HasPrefix(ident, "https://", true /* ignore case */) ||
      HasPrefix(ident, "file://", true /* ignore case */))
  {
    std::string fqrn = Repository::GetFqrnFromUrl(ident);
    sanitizer::RepositorySanitizer sanitizer;
    if (!sanitizer.IsValid(fqrn)) {
      throw EPublish("malformed repository name: " + fqrn);
    }
    SettingsRepository settings(fqrn);
    settings.SetUrl(ident);
    return settings;
  }

  std::string alias = ident.empty() ? GetSingleAlias() : ident;
  std::string repo_path = config_path_ + "/" + alias;
  std::string server_path = repo_path + "/server.conf";
  std::string replica_path = repo_path + "/replica.conf";
  std::string fqrn = alias;

  delete options_mgr_;
  options_mgr_ = new BashOptionsManager();
  std::string arg;
  options_mgr_->set_taint_environment(false);
  options_mgr_->ParsePath(server_path, false /* external */);
  options_mgr_->ParsePath(replica_path, false /* external */);
  if (options_mgr_->GetValue("CVMFS_REPOSITORY_NAME", &arg))
    fqrn = arg;
  SettingsRepository settings(fqrn);

  if (options_mgr_->GetValue("CVMFS_PUBLIC_KEY", &arg))
    settings.GetKeychain()->SetKeychainDir(arg);
  if (options_mgr_->GetValue("CVMFS_STRATUM0", &arg))
    settings.SetUrl(arg);
  // For a replica, the stratum 1 url is the "local" location, hence it takes
  // precedence over the stratum 0 url
  if (options_mgr_->GetValue("CVMFS_STRATUM1", &arg))
    settings.SetUrl(arg);
  if (options_mgr_->GetValue("CVMFS_SPOOL_DIR", &arg))
    settings.SetTmpDir(arg + "/tmp");

  return settings;
}

const std::string GetFQRN(const std::string &ident) {
  std::string fqrn;
  sanitizer::RepositorySanitizer sanitizer;

  if (HasPrefix(ident, "http://", true /* ignore case */) ||
      HasPrefix(ident, "https://", true /* ignore case */) ||
      HasPrefix(ident, "file://", true /* ignore case */)) {
    fqrn = Repository::GetFqrnFromUrl(ident);
  } else if (ident.empty()) {
    fqrn = GetSingleAlias();
  } else {
    fqrn = ident;
  }

  if (!sanitizer.IsValid(fqrn)) {
    throw EPublish("malformed repository name: " + fqrn);
  }
  return fqrn;
}

bool SettingsBuilder::CreateSettingsReplicas(const std::string &ident,
                                             SettingsReplica &out) const {
  std::string fqrn = GetFQRN(ident);
  std::string repo_path = config_path_ + "/" + fqrn;

  // if the replica file does not exists we cannot have a Replica
  std::string replica_path = repo_path + "/replica.conf";
  platform_stat64 info;
  if (!platform_stat(replica_path.c_str(), &info)) {
    return false;
  }

  SettingsPublisher spub(fqrn);
  return true;
}

}  // namespace publish
