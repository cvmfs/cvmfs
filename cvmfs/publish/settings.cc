/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "publish/settings.h"

#include <unistd.h>

#include <cstdlib>
#include <string>
#include <vector>

#include "hash.h"
#include "options.h"
#include "publish/except.h"
#include "publish/repository.h"
#include "sanitizer.h"
#include "util/pointer.h"
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

void SettingsSpoolArea::SetRepairMode(const EUnionMountRepairMode val) {
  repair_mode_ = val;
}

void SettingsSpoolArea::EnsureDirectories() {
  std::vector<std::string> targets;
  targets.push_back(tmp_dir());
  targets.push_back(readonly_mnt());
  targets.push_back(scratch_dir());
  targets.push_back(cache_dir());
  targets.push_back(log_dir());
  targets.push_back(ovl_work_dir());

  for (unsigned i = 0; i < targets.size(); ++i) {
    bool rv = MkdirDeep(targets[i], 0700, true /* veryfy_writable */);
    if (!rv)
      throw publish::EPublish("cannot create directory " + targets[i]);
  }
}


//------------------------------------------------------------------------------


void SettingsTransaction::SetLayoutRevision(const unsigned revision) {
  layout_revision_ = revision;
}

void SettingsTransaction::SetInEnterSession(const bool value) {
  in_enter_session_ = value;
}

void SettingsTransaction::SetBaseHash(const shash::Any &hash) {
  base_hash_ = hash;
}

void SettingsTransaction::SetHashAlgorithm(const std::string &algorithm) {
  hash_algorithm_ = shash::ParseHashAlgorithm(algorithm);
}

void SettingsTransaction::SetCompressionAlgorithm(const std::string &algorithm)
{
  compression_algorithm_ = zlib::ParseCompressionAlgorithm(algorithm);
}

void SettingsTransaction::SetEnforceLimits(bool value) {
  enforce_limits_ = value;
}

void SettingsTransaction::SetLimitNestedCatalogKentries(unsigned value) {
  limit_nested_catalog_kentries_ = value;
}

void SettingsTransaction::SetLimitRootCatalogKentries(unsigned value) {
  limit_root_catalog_kentries_ = value;
}

void SettingsTransaction::SetLimitFileSizeMb(unsigned value) {
  limit_file_size_mb_ = value;
}

void SettingsTransaction::SetUseCatalogAutobalance(bool value) {
  use_catalog_autobalance_ = value;
}

void SettingsTransaction::SetAutobalanceMaxWeight(unsigned value) {
  autobalance_max_weight_ = value;
}

void SettingsTransaction::SetAutobalanceMinWeight(unsigned value) {
  autobalance_min_weight_ = value;
}

void SettingsTransaction::SetPrintChangeset(bool value) {
  print_changeset_ = value;
}

void SettingsTransaction::SetDryRun(bool value) {
  dry_run_ = value;
}

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

void SettingsTransaction::SetTimeout(unsigned seconds) {
  timeout_s_ = seconds;
}

int SettingsTransaction::GetTimeoutS() const {
  if (timeout_s_.is_default())
    return -1;
  return timeout_s_;
}

void SettingsTransaction::SetLeasePath(const std::string &path) {
  lease_path_ = path;
}

void SettingsTransaction::SetTemplate(
  const std::string &from, const std::string &to)
{
  if (from.empty())
    throw EPublish("template transaction's 'from' path must not be empty");
  if (to.empty())
    throw EPublish("template transaction's 'to' path must not be empty");
  template_from_ = (from[0] == '/') ? from.substr(1) : from;
  template_to_ = (to[0] == '/') ? to.substr(1) : to;
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

void SettingsStorage::MakeGateway(
  const std::string &host,
  unsigned int port,
  const std::string &tmp_dir)
{
  type_ = upload::SpoolerDefinition::Gateway;
  endpoint_ = "http://" + host + ":" + StringifyInt(port) + "/api/v1";
  tmp_dir_ = tmp_dir_;
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
  gw_key_path_ = keychain_dir + "/" + fqrn_() + ".gw";
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

bool SettingsKeychain::HasGatewayKey() const {
  return FileExists(gw_key_path_);
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
    throw EPublish("unknown user name for repository owner: " + user_name);
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


std::map<std::string, std::string> SettingsBuilder::GetSessionEnvironment() {
  std::map<std::string, std::string> result;
  std::string session_dir = Env::GetEnterSessionDir();
  if (session_dir.empty())
    return result;

  // Get the repository name from the ephemeral writable shell
  BashOptionsManager omgr;
  omgr.set_taint_environment(false);
  omgr.ParsePath(session_dir + "/env.conf", false /* external */);

  // We require at least CVMFS_FQRN to be set
  std::string fqrn;
  if (!omgr.GetValue("CVMFS_FQRN", &fqrn)) {
    throw EPublish("no repositories found in ephemeral writable shell",
                   EPublish::kFailInvocation);
  }

  std::vector<std::string> keys = omgr.GetAllKeys();
  for (unsigned i = 0; i < keys.size(); ++i) {
    result[keys[i]] = omgr.GetValueOrDie(keys[i]);
  }
  return result;
}


std::string SettingsBuilder::GetSingleAlias() {
  std::map<std::string, std::string> session_env = GetSessionEnvironment();
  if (!session_env.empty())
    return session_env["CVMFS_FQRN"];

  std::vector<std::string> repositories = FindDirectories(config_path_);
  if (repositories.empty()) {
    throw EPublish("no repositories available in " + config_path_,
                   EPublish::kFailInvocation);
  }

  for (unsigned i = 0; i < repositories.size(); ++i) {
    repositories[i] = GetFileName(repositories[i]);
  }
  if (repositories.size() > 1) {
    throw EPublish("multiple repositories available in " + config_path_ +
                   ":\n  " + JoinStrings(repositories, "\n  "),
                   EPublish::kFailInvocation);
  }
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

std::string SettingsPublisher::GetReadOnlyXAttr(const std::string &attr) {
  std::string value;
  bool rvb = platform_getxattr(this->transaction().spool_area().readonly_mnt(),
                               attr, &value);
  if (!rvb) {
    throw EPublish("cannot get extended attribute " + attr);
  }
  return value;
}

SettingsPublisher* SettingsBuilder::CreateSettingsPublisherFromSession() {
  std::string session_dir = Env::GetEnterSessionDir();
  std::map<std::string, std::string> session_env = GetSessionEnvironment();
  std::string fqrn = session_env["CVMFS_FQRN"];

  UniquePtr<SettingsPublisher> settings_publisher(
      new SettingsPublisher(SettingsRepository(fqrn)));
  // TODO(jblomer): work in progress
  settings_publisher->GetTransaction()->SetInEnterSession(true);
  settings_publisher->GetTransaction()->GetSpoolArea()->SetSpoolArea(
    session_dir);

  std::string base_hash =
    settings_publisher->GetReadOnlyXAttr("user.root_hash");

  BashOptionsManager omgr;
  omgr.set_taint_environment(false);
  omgr.ParsePath(settings_publisher->transaction().spool_area().client_config(),
                 false /* external */);

  std::string arg;
  settings_publisher->SetUrl(settings_publisher->GetReadOnlyXAttr("user.host"));
  if (omgr.GetValue("CVMFS_KEYS_DIR", &arg))
    settings_publisher->GetKeychain()->SetKeychainDir(arg);
  settings_publisher->GetTransaction()->SetLayoutRevision(
    Publisher::kRequiredLayoutRevision);
  settings_publisher->GetTransaction()->SetBaseHash(shash::MkFromHexPtr(
    shash::HexPtr(base_hash), shash::kSuffixCatalog));
  settings_publisher->GetTransaction()->SetUnionFsType("overlayfs");
  settings_publisher->SetOwner(geteuid(), getegid());

  return settings_publisher.Release();
}


SettingsPublisher* SettingsBuilder::CreateSettingsPublisher(
  const std::string &ident, bool needs_managed)
{
  // we are creating a publisher, it need to have the `server.conf` file
  // present, otherwise something is wrong and we should exit early
  const std::string alias(ident.empty() ? GetSingleAlias() : ident);

  std::map<std::string, std::string> session_env = GetSessionEnvironment();
  // We can be in an ephemeral writable shell but interested in a different
  // repository
  if (!session_env.empty() && (session_env["CVMFS_FQRN"] == alias))
    return CreateSettingsPublisherFromSession();

  const std::string server_path = config_path_ + "/" + alias + "/server.conf";

  if (FileExists(server_path) == false) {
    throw EPublish(
        "Unable to find the configuration file `server.conf` for the cvmfs "
        "publisher: " + alias,
        EPublish::kFailRepositoryNotFound);
  }

  SettingsRepository settings_repository = CreateSettingsRepository(alias);
  if (needs_managed && !IsManagedRepository())
    throw EPublish("remote repositories are not supported in this context");

  if (options_mgr_->GetValueOrDie("CVMFS_REPOSITORY_TYPE") != "stratum0") {
    throw EPublish("Repository " + alias + " is not a stratum 0 repository",
                   EPublish::kFailRepositoryType);
  }

  UniquePtr<SettingsPublisher> settings_publisher(
      new SettingsPublisher(settings_repository));

  try {
    std::string xattr = settings_publisher->GetReadOnlyXAttr("user.root_hash");
    settings_publisher->GetTransaction()->SetBaseHash(
        shash::MkFromHexPtr(shash::HexPtr(xattr), shash::kSuffixCatalog));
  } catch (const EPublish& e) {
    // We ignore the exception.
    // In case of exception, the base hash remains unset.
  }

  settings_publisher->SetIsManaged(IsManagedRepository());
  settings_publisher->SetOwner(options_mgr_->GetValueOrDie("CVMFS_USER"));
  settings_publisher->GetStorage()->SetLocator(
    options_mgr_->GetValueOrDie("CVMFS_UPSTREAM_STORAGE"));

  std::string arg;
  if (options_mgr_->GetValue("CVMFS_CREATOR_VERSION", &arg)) {
    settings_publisher->GetTransaction()->SetLayoutRevision(String2Uint64(arg));
  }
  if (options_mgr_->GetValue("CVMFS_UNION_FS_TYPE", &arg)) {
    settings_publisher->GetTransaction()->SetUnionFsType(arg);
  }
  if (options_mgr_->GetValue("CVMFS_HASH_ALGORITHM", &arg)) {
    settings_publisher->GetTransaction()->SetHashAlgorithm(arg);
  }
  if (options_mgr_->GetValue("CVMFS_COMPRESSION_ALGORITHM", &arg)) {
    settings_publisher->GetTransaction()->SetCompressionAlgorithm(arg);
  }
  if (options_mgr_->GetValue("CVMFS_ENFORCE_LIMITS", &arg)) {
    settings_publisher->GetTransaction()->SetEnforceLimits(
      options_mgr_->IsOn(arg));
  }
  if (options_mgr_->GetValue("CVMFS_NESTED_KCATALOG_LIMIT", &arg)) {
    settings_publisher->GetTransaction()->SetLimitNestedCatalogKentries(
      String2Uint64(arg));
  }
  if (options_mgr_->GetValue("CVMFS_ROOT_KCATALOG_LIMIT", &arg)) {
    settings_publisher->GetTransaction()->SetLimitRootCatalogKentries(
      String2Uint64(arg));
  }
  if (options_mgr_->GetValue("CVMFS_FILE_MBYTE_LIMIT", &arg)) {
    settings_publisher->GetTransaction()->SetLimitFileSizeMb(
      String2Uint64(arg));
  }
  if (options_mgr_->GetValue("CVMFS_AUTOCATALOGS", &arg)) {
    settings_publisher->GetTransaction()->SetUseCatalogAutobalance(
      options_mgr_->IsOn(arg));
  }
  if (options_mgr_->GetValue("CVMFS_AUTOCATALOGS_MAX_WEIGHT", &arg)) {
    settings_publisher->GetTransaction()->SetAutobalanceMaxWeight(
      String2Uint64(arg));
  }
  if (options_mgr_->GetValue("CVMFS_AUTOCATALOGS_MIN_WEIGHT", &arg)) {
    settings_publisher->GetTransaction()->SetAutobalanceMinWeight(
      String2Uint64(arg));
  }
  if (options_mgr_->GetValue("CVMFS_AUTO_REPAIR_MOUNTPOINT", &arg)) {
    if (!options_mgr_->IsOn(arg)) {
      settings_publisher->GetTransaction()->GetSpoolArea()->SetRepairMode(
        kUnionMountRepairNever);
    }
  }

  // TODO(jblomer): process other parameters
  return settings_publisher.Release();
}

}  // namespace publish
