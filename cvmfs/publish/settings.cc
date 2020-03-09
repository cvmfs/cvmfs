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


void SettingsSpoolArea::SetUnionMount(const std::string &path) {
  union_mnt_ = path;
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


const unsigned SettingsPublisher::kDefaultWhitelistValidity = 30;


SettingsPublisher::SettingsPublisher(
  const SettingsRepository &settings_repository)
  : fqrn_(settings_repository.fqrn())
  , url_(settings_repository.url())
  , owner_uid_(0)
  , owner_gid_(0)
  , whitelist_validity_days_(kDefaultWhitelistValidity)
  , is_silent_(false)
  , is_managed_(false)
  , storage_(fqrn_)
  , transaction_(fqrn_)
  , keychain_(fqrn_)
{
  keychain_.SetKeychainDir(settings_repository.keychain().keychain_dir());
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

void SettingsPublisher::SetIsManaged(bool value) {
  is_managed_ = value;
}


//------------------------------------------------------------------------------


SettingsBuilder::~SettingsBuilder() {
  delete options_mgr_;
}


std::string SettingsBuilder::GetSingleAlias() {
  std::vector<std::string> repositories = FindDirectories(config_path_);
  if (repositories.empty())
    throw EPublish("no repositories available in " + config_path_);
  if (repositories.size() > 1)
    throw EPublish("multiple repositories available in " + config_path_);
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


SettingsPublisher SettingsBuilder::CreateSettingsPublisher(
  const std::string &ident, bool needs_managed)
{
  SettingsRepository settings_repository = CreateSettingsRepository(ident);
  if (needs_managed && !IsManagedRepository())
    throw EPublish("remote repositories are not supported in this context");

  // TODO(jblomer): fix for remote repositories
  if (options_mgr_->GetValueOrDie("CVMFS_REPOSITORY_TYPE") != "stratum0")
    throw EPublish("Not a stratum 0 repository");

  SettingsPublisher settings_publisher(settings_repository);
  settings_publisher.SetIsManaged(IsManagedRepository());
  settings_publisher.SetOwner(options_mgr_->GetValueOrDie("CVMFS_USER"));
  settings_publisher.GetStorage()->SetLocator(
    options_mgr_->GetValueOrDie("CVMFS_UPSTREAM_STORAGE"));

  // TODO(jblomer): process other parameters

  return settings_publisher;
}

}  // namespace publish
