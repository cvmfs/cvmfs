/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "publish/repository.h"

#include <cassert>
#include <cstddef>
#include <cstdlib>

#include "catalog_mgr_ro.h"
#include "catalog_mgr_rw.h"
#include "download.h"
#include "gateway_util.h"
#include "hash.h"
#include "history_sqlite.h"
#include "ingestion/ingestion_source.h"
#include "logging.h"
#include "manifest.h"
#include "manifest_fetch.h"
#include "publish/except.h"
#include "publish/repository_util.h"
#include "publish/settings.h"
#include "reflog.h"
#include "signature.h"
#include "statistics.h"
#include "sync_mediator.h"
#include "sync_union_aufs.h"
#include "sync_union_overlayfs.h"
#include "sync_union_tarball.h"
#include "upload.h"
#include "upload_spooler_definition.h"
#include "util/pointer.h"
#include "whitelist.h"

// TODO(jblomer): Remove Me
namespace swissknife {
class CommandTag {
  static const std::string kHeadTag;
  static const std::string kPreviousHeadTag;
};
const std::string CommandTag::kHeadTag = "trunk";
const std::string CommandTag::kPreviousHeadTag = "trunk-previous";
}

namespace publish {

Repository::Repository()
  : settings_("")
  , statistics_(new perf::Statistics())
  , signature_mgr_(new signature::SignatureManager())
  , download_mgr_(NULL)
  , simple_catalog_mgr_(NULL)
  , whitelist_(NULL)
  , reflog_(NULL)
  , manifest_(NULL)
  , history_(NULL)
{
  signature_mgr_->Init();
}

Repository::Repository(const SettingsRepository &settings)
  : settings_(settings)
  , statistics_(new perf::Statistics())
  , signature_mgr_(new signature::SignatureManager())
  , download_mgr_(NULL)
  , simple_catalog_mgr_(NULL)
  , whitelist_(NULL)
  , reflog_(NULL)
  , manifest_(NULL)
  , history_(NULL)
{
  signature_mgr_->Init();

  int rvb;
  std::string keys = JoinStrings(FindFilesBySuffix(
    settings.keychain().keychain_dir(), ".pub"), ":");
  rvb = signature_mgr_->LoadPublicRsaKeys(keys);
  if (!rvb) {
    signature_mgr_->Fini();
    delete signature_mgr_;
    delete statistics_;
    throw EPublish("cannot load public rsa key");
  }

  download_mgr_ = new download::DownloadManager();
  download_mgr_->Init(16, false,
                      perf::StatisticsTemplate("download", statistics_));
  try {
    DownloadRootObjects(settings.url(), settings.fqrn(), settings.tmp_dir());
  } catch (const EPublish& e) {
    signature_mgr_->Fini();
    download_mgr_->Fini();
    delete signature_mgr_;
    delete download_mgr_;
    delete statistics_;
    throw;
  }
}

Repository::~Repository() {
  if (signature_mgr_ != NULL) signature_mgr_->Fini();
  if (download_mgr_ != NULL) download_mgr_->Fini();

  delete history_;
  delete manifest_;
  delete reflog_;
  delete whitelist_;
  delete signature_mgr_;
  delete download_mgr_;
  delete simple_catalog_mgr_;
  delete statistics_;
}

const history::History *Repository::history() const { return history_; }

catalog::SimpleCatalogManager *Repository::GetSimpleCatalogManager() {
  if (simple_catalog_mgr_ != NULL) return simple_catalog_mgr_;

  simple_catalog_mgr_ = new catalog::SimpleCatalogManager(
    manifest_->catalog_hash(),
    settings_.url(),
    settings_.tmp_dir(),
    download_mgr_,
    statistics_,
    true /* manage_catalog_files */);
  simple_catalog_mgr_->Init();
  return simple_catalog_mgr_;
}


void Repository::DownloadRootObjects(
  const std::string &url, const std::string &fqrn, const std::string &tmp_dir)
{
  delete whitelist_;
  whitelist_ = new whitelist::Whitelist(fqrn, download_mgr_, signature_mgr_);
  whitelist::Failures rv_whitelist = whitelist_->LoadUrl(url);
  if (whitelist_->status() != whitelist::Whitelist::kStAvailable) {
    throw EPublish(std::string("cannot load whitelist [") +
                   whitelist::Code2Ascii(rv_whitelist) + "]");
  }

  manifest::ManifestEnsemble ensemble;
  const uint64_t minimum_timestamp = 0;
  const shash::Any *base_catalog = NULL;
  manifest::Failures rv_manifest = manifest::Fetch(
    url, fqrn, minimum_timestamp, base_catalog, signature_mgr_, download_mgr_,
    &ensemble);
  if (rv_manifest != manifest::kFailOk) throw EPublish("cannot load manifest");
  delete manifest_;
  manifest_ = new manifest::Manifest(*ensemble.manifest);

  std::string reflog_path;
  FILE *reflog_fd =
    CreateTempFile(tmp_dir + "/reflog", kPrivateFileMode, "w", &reflog_path);
  std::string reflog_url = url + "/.cvmfsreflog";
  // TODO(jblomer): verify reflog hash
  // shash::Any reflog_hash(manifest_->GetHashAlgorithm());
  download::JobInfo download_reflog(
       &reflog_url,
       false /* compressed */,
       false /* probe hosts */,
       reflog_fd,
       NULL);
  download::Failures rv_dl = download_mgr_->Fetch(&download_reflog);
  fclose(reflog_fd);
  if (rv_dl == download::kFailOk) {
    delete reflog_;
    reflog_ = manifest::Reflog::Open(reflog_path);
    if (reflog_ == NULL) throw EPublish("cannot open reflog");
    reflog_->TakeDatabaseFileOwnership();
  } else {
    if (!download_reflog.IsFileNotFound()) {
      throw EPublish(std::string("cannot load reflog [") +
                     download::Code2Ascii(rv_dl) + "]");
    }
    assert(reflog_ == NULL);
  }

  std::string tags_path;
  FILE *tags_fd =
    CreateTempFile(tmp_dir + "/tags", kPrivateFileMode, "w", &tags_path);
  if (!manifest_->history().IsNull()) {
    std::string tags_url = url + "/data/" + manifest_->history().MakePath();
    shash::Any tags_hash(manifest_->history());
    download::JobInfo download_tags(
         &tags_url,
         true /* compressed */,
         true /* probe hosts */,
         tags_fd,
         &tags_hash);
    rv_dl = download_mgr_->Fetch(&download_tags);
    fclose(tags_fd);
    if (rv_dl != download::kFailOk) throw EPublish("cannot load tag database");
    delete history_;
    history_ = history::SqliteHistory::OpenWritable(tags_path);
    if (history_ == NULL) throw EPublish("cannot open tag database");
  } else {
    fclose(tags_fd);
    delete history_;
    history_ = history::SqliteHistory::Create(tags_path, fqrn);
    if (history_ == NULL) throw EPublish("cannot create tag database");
  }
  history_->TakeDatabaseFileOwnership();

  if (!manifest_->meta_info().IsNull()) {
    shash::Any info_hash(manifest_->meta_info());
    std::string info_url = url + "/data/" + info_hash.MakePath();
    download::JobInfo download_info(
      &info_url,
      true /* compressed */,
      true /* probe_hosts */,
      &info_hash);
    download::Failures rv_info = download_mgr_->Fetch(&download_info);
    if (rv_info != download::kFailOk) {
      throw EPublish(std::string("cannot load meta info [") +
                     download::Code2Ascii(rv_info) + "]");
    }
    meta_info_ = std::string(download_info.destination_mem.data,
                             download_info.destination_mem.pos);
  } else {
    meta_info_ = "n/a";
  }
}


std::string Repository::GetFqrnFromUrl(const std::string &url) {
  return GetFileName(MakeCanonicalPath(url));
}


bool Repository::IsMasterReplica() {
  std::string url = settings_.url() + "/.cvmfs_master_replica";
  download::JobInfo head(&url, false /* probe_hosts */);
  download::Failures retval = download_mgr_->Fetch(&head);
  if (retval == download::kFailOk) return true;
  if (head.IsFileNotFound()) return false;

  throw EPublish(std::string("error looking for .cvmfs_master_replica [") +
                 download::Code2Ascii(retval) + "]");
}


//------------------------------------------------------------------------------


void Publisher::ConstructSpoolers() {
  if ((spooler_files_ != NULL) && (spooler_catalogs_ != NULL))
    return;
  assert((spooler_files_ == NULL) && (spooler_catalogs_ == NULL));

  upload::SpoolerDefinition sd(
    settings_.storage().GetLocator(),
    settings_.transaction().hash_algorithm(),
    settings_.transaction().compression_algorithm());
  sd.session_token_file =
    settings_.transaction().spool_area().gw_session_token();
  sd.key_file = settings_.keychain().gw_key_path();

  spooler_files_ = upload::Spooler::Construct(sd, statistics_publish_);
  if (spooler_files_ == NULL)
    throw EPublish("could not initialize file spooler");

  upload::SpoolerDefinition sd_catalogs(sd.Dup2DefaultCompression());
  spooler_catalogs_ =
    upload::Spooler::Construct(sd_catalogs, statistics_publish_);
  if (spooler_catalogs_ == NULL) {
    delete spooler_files_;
    throw EPublish("could not initialize catalog spooler");
  }
}


void Publisher::CreateKeychain() {
  if (settings_.keychain().HasDanglingMasterKeys()) {
    throw EPublish("dangling master key pair");
  }
  if (settings_.keychain().HasDanglingRepositoryKeys()) {
    throw EPublish("dangling repository keys");
  }
  if (!settings_.keychain().HasMasterKeys())
    signature_mgr_->GenerateMasterKeyPair();
  if (!settings_.keychain().HasRepositoryKeys())
    signature_mgr_->GenerateCertificate(settings_.fqrn());

  whitelist_ = new whitelist::Whitelist(settings_.fqrn(), NULL, signature_mgr_);
  std::string whitelist_str = whitelist::Whitelist::CreateString(
    settings_.fqrn(),
    settings_.whitelist_validity_days(),
    settings_.transaction().hash_algorithm(),
    signature_mgr_);
  whitelist::Failures rv_wl = whitelist_->LoadMem(whitelist_str);
  if (rv_wl != whitelist::kFailOk)
    throw EPublish("whitelist generation failed");
}


void Publisher::CreateRootObjects() {
  // Reflog
  const std::string reflog_path = CreateTempPath(
    settings_.transaction().spool_area().tmp_dir() + "/cvmfs_reflog", 0600);
  reflog_ = manifest::Reflog::Create(reflog_path, settings_.fqrn());
  if (reflog_ == NULL) throw EPublish("could not create reflog");
  reflog_->TakeDatabaseFileOwnership();

  // Root file catalog and initial manifest
  manifest_ = catalog::WritableCatalogManager::CreateRepository(
    settings_.transaction().spool_area().tmp_dir(),
    settings_.transaction().is_volatile(),
    settings_.transaction().voms_authz(),
    spooler_catalogs_);
  spooler_catalogs_->WaitForUpload();
  if (manifest_ == NULL)
    throw EPublish("could not create initial file catalog");
  reflog_->AddCatalog(manifest_->catalog_hash());

  manifest_->set_repository_name(settings_.fqrn());
  manifest_->set_ttl(settings_.transaction().ttl_second());
  const bool needs_bootstrap_shortcuts =
    !settings_.transaction().voms_authz().empty();
  manifest_->set_has_alt_catalog_path(needs_bootstrap_shortcuts);
  manifest_->set_garbage_collectability(
    settings_.transaction().is_garbage_collectable());

  // Tag database
  const std::string tags_path = CreateTempPath(
    settings_.transaction().spool_area().tmp_dir() + "/cvmfs_tags", 0600);
  history_ = history::SqliteHistory::Create(tags_path, settings_.fqrn());
  if (history_ == NULL) throw EPublish("could not create tag database");
  history_->TakeDatabaseFileOwnership();
  history::History::Tag tag_trunk(
    "trunk",
    manifest_->catalog_hash(), manifest_->catalog_size(), manifest_->revision(),
    manifest_->publish_timestamp(), history::History::kChannelTrunk,
    "empty repository", "" /* branch */);
  history_->Insert(tag_trunk);

  // Meta information, TODO(jblomer)
  meta_info_ = "{}";
}


void Publisher::CreateStorage() {
  ConstructSpoolers();
  if (!spooler_files_->Create())
    throw EPublish("could not initialize repository storage area");
}


void Publisher::PushCertificate() {
  upload::Spooler::CallbackPtr callback =
    spooler_files_->RegisterListener(&Publisher::OnProcessCertificate, this);
  spooler_files_->ProcessCertificate(
    new StringIngestionSource(signature_mgr_->GetCertificate()));
  spooler_files_->WaitForUpload();
  spooler_files_->UnregisterListener(callback);
}


void Publisher::PushHistory() {
  assert(history_ != NULL);
  history_->SetPreviousRevision(manifest_->history());
  const string history_path = history_->filename();
  history_->DropDatabaseFileOwnership();
  delete history_;

  upload::Spooler::CallbackPtr callback =
    spooler_files_->RegisterListener(&Publisher::OnProcessHistory, this);
  spooler_files_->ProcessHistory(history_path);
  spooler_files_->WaitForUpload();
  spooler_files_->UnregisterListener(callback);

  history_ = history::SqliteHistory::OpenWritable(history_path);
  assert(history_ != NULL);
  history_->TakeDatabaseFileOwnership();
}


void Publisher::PushMetainfo() {
  upload::Spooler::CallbackPtr callback =
    spooler_files_->RegisterListener(&Publisher::OnProcessMetainfo, this);
  spooler_files_->ProcessMetainfo(new StringIngestionSource(meta_info_));
  spooler_files_->WaitForUpload();
  spooler_files_->UnregisterListener(callback);
}


void Publisher::PushManifest() {
  std::string signed_manifest = manifest_->ExportString();
  shash::Any manifest_hash(settings_.transaction().hash_algorithm());
  shash::HashMem(
      reinterpret_cast<const unsigned char *>(signed_manifest.data()),
      signed_manifest.length(), &manifest_hash);
  signed_manifest += "--\n" + manifest_hash.ToString() + "\n";
  unsigned char *signature;
  unsigned sig_size;
  bool rvb = signature_mgr_->Sign(
    reinterpret_cast<const unsigned char *>(manifest_hash.ToString().data()),
    manifest_hash.GetHexSize(), &signature, &sig_size);
  if (!rvb) throw EPublish("cannot sign manifest");
  signed_manifest += std::string(reinterpret_cast<char *>(signature), sig_size);
  free(signature);

  // Create alternative bootstrapping symlinks for VOMS secured repos
  if (manifest_->has_alt_catalog_path()) {
    rvb =
      spooler_files_->PlaceBootstrappingShortcut(manifest_->certificate()) &&
      spooler_files_->PlaceBootstrappingShortcut(manifest_->catalog_hash()) &&
      (manifest_->history().IsNull() ||
        spooler_files_->PlaceBootstrappingShortcut(manifest_->history())) &&
      (manifest_->meta_info().IsNull() ||
        spooler_files_->PlaceBootstrappingShortcut(manifest_->meta_info()));
    if (!rvb) EPublish("cannot place VOMS bootstrapping symlinks");
  }

  upload::Spooler::CallbackPtr callback =
    spooler_files_->RegisterListener(&Publisher::OnUploadManifest, this);
  spooler_files_->Upload(".cvmfspublished",
                   new StringIngestionSource(signed_manifest));
  spooler_files_->WaitForUpload();
  spooler_files_->UnregisterListener(callback);
}


void Publisher::PushReflog() {
  const string reflog_path = reflog_->database_file();
  reflog_->DropDatabaseFileOwnership();
  delete reflog_;

  shash::Any hash_reflog(settings_.transaction().hash_algorithm());
  manifest::Reflog::HashDatabase(reflog_path, &hash_reflog);

  upload::Spooler::CallbackPtr callback =
    spooler_files_->RegisterListener(&Publisher::OnUploadReflog, this);
  spooler_files_->UploadReflog(reflog_path);
  spooler_files_->WaitForUpload();
  spooler_files_->UnregisterListener(callback);

  manifest_->set_reflog_hash(hash_reflog);

  reflog_ = manifest::Reflog::Open(reflog_path);
  assert(reflog_ != NULL);
  reflog_->TakeDatabaseFileOwnership();
}


void Publisher::PushWhitelist() {
  // TODO(jblomer): PKCS7 handling
  upload::Spooler::CallbackPtr callback =
    spooler_files_->RegisterListener(&Publisher::OnUploadWhitelist, this);
  spooler_files_->Upload(".cvmfswhitelist",
                         new StringIngestionSource(whitelist_->ExportString()));
  spooler_files_->WaitForUpload();
  spooler_files_->UnregisterListener(callback);
}


Publisher *Publisher::Create(const SettingsPublisher &settings) {
  UniquePtr<Publisher> publisher(new Publisher());

  publisher->settings_ = settings;
  if (settings.is_silent())
    publisher->llvl_ = kLogNone;
  publisher->signature_mgr_ = new signature::SignatureManager();
  publisher->signature_mgr_->Init();

  LogCvmfs(kLogCvmfs, publisher->llvl_ | kLogStdout | kLogNoLinebreak,
           "Creating Key Chain... ");
  publisher->CreateKeychain();
  publisher->ExportKeychain();
  LogCvmfs(kLogCvmfs, publisher->llvl_ | kLogStdout, "done");

  LogCvmfs(kLogCvmfs, publisher->llvl_ | kLogStdout | kLogNoLinebreak,
           "Creating Backend Storage... ");
  publisher->CreateStorage();
  publisher->PushWhitelist();
  LogCvmfs(kLogCvmfs, publisher->llvl_ | kLogStdout, "done");

  LogCvmfs(kLogCvmfs, publisher->llvl_ | kLogStdout | kLogNoLinebreak,
           "Creating Initial Repository... ");
  publisher->InitSpoolArea();
  publisher->CreateRootObjects();
  publisher->PushHistory();
  publisher->PushCertificate();
  publisher->PushMetainfo();
  publisher->PushReflog();
  publisher->PushManifest();
  // TODO(jblomer): meta-info

  // Re-create from empty repository in order to properly initialize
  // parent Repository object
  publisher = new Publisher(settings);

  LogCvmfs(kLogCvmfs, publisher->llvl_ | kLogStdout, "done");

  return publisher.Release();
}

void Publisher::ExportKeychain() {
  CreateDirectoryAsOwner(settings_.keychain().keychain_dir(), kDefaultDirMode);

  bool rvb;
  rvb = SafeWriteToFile(signature_mgr_->GetActivePubkeys(),
                        settings_.keychain().master_public_key_path(), 0644);
  if (!rvb) throw EPublish("cannot export public master key");
  rvb = SafeWriteToFile(signature_mgr_->GetCertificate(),
                        settings_.keychain().certificate_path(), 0644);
  if (!rvb) throw EPublish("cannot export certificate");

  rvb = SafeWriteToFile(signature_mgr_->GetPrivateKey(),
                        settings_.keychain().private_key_path(), 0600);
  if (!rvb) throw EPublish("cannot export private certificate key");
  rvb = SafeWriteToFile(signature_mgr_->GetPrivateMasterKey(),
                        settings_.keychain().master_private_key_path(), 0600);
  if (!rvb) throw EPublish("cannot export private master key");

  int rvi;
  rvi = chown(settings_.keychain().master_public_key_path().c_str(),
              settings_.owner_uid(), settings_.owner_gid());
  if (rvi != 0) throw EPublish("cannot set key file ownership");
  rvi = chown(settings_.keychain().certificate_path().c_str(),
              settings_.owner_uid(), settings_.owner_gid());
  if (rvi != 0) throw EPublish("cannot set key file ownership");
  rvi = chown(settings_.keychain().private_key_path().c_str(),
              settings_.owner_uid(), settings_.owner_gid());
  if (rvi != 0) throw EPublish("cannot set key file ownership");
  rvi = chown(settings_.keychain().master_private_key_path().c_str(),
              settings_.owner_uid(), settings_.owner_gid());
  if (rvi != 0) throw EPublish("cannot set key file ownership");
}

void Publisher::OnProcessCertificate(const upload::SpoolerResult &result) {
  if (result.return_code != 0) {
    throw EPublish("cannot write certificate to storage");
  }
  manifest_->set_certificate(result.content_hash);
  reflog_->AddCertificate(result.content_hash);
}

void Publisher::OnProcessHistory(const upload::SpoolerResult &result) {
  if (result.return_code != 0) {
    throw EPublish("cannot write tag database to storage");
  }
  manifest_->set_history(result.content_hash);
  reflog_->AddHistory(result.content_hash);
}

void Publisher::OnProcessMetainfo(const upload::SpoolerResult &result) {
  if (result.return_code != 0) {
    throw EPublish("cannot write repository meta info to storage");
  }
  manifest_->set_meta_info(result.content_hash);
  reflog_->AddMetainfo(result.content_hash);
}

void Publisher::OnUploadManifest(const upload::SpoolerResult &result) {
  if (result.return_code != 0) {
    throw EPublish("cannot write manifest to storage");
  }
}

void Publisher::OnUploadReflog(const upload::SpoolerResult &result) {
  if (result.return_code != 0) {
    throw EPublish("cannot write reflog to storage");
  }
}

void Publisher::OnUploadWhitelist(const upload::SpoolerResult &result) {
  if (result.return_code != 0) {
    throw EPublish("cannot write whitelist to storage");
  }
}

void Publisher::CreateDirectoryAsOwner(const std::string &path, int mode)
{
  bool rvb = MkdirDeep(path, kPrivateDirMode);
  if (!rvb) throw EPublish("cannot create directory " + path);
  int rvi = chown(path.c_str(), settings_.owner_uid(), settings_.owner_gid());
  if (rvi != 0) throw EPublish("cannot set ownership on directory " + path);
}

void Publisher::InitSpoolArea() {
  CreateDirectoryAsOwner(settings_.transaction().spool_area().workspace(),
                         kPrivateDirMode);
  CreateDirectoryAsOwner(settings_.transaction().spool_area().tmp_dir(),
                         kPrivateDirMode);
  CreateDirectoryAsOwner(settings_.transaction().spool_area().cache_dir(),
                         kPrivateDirMode);
  CreateDirectoryAsOwner(settings_.transaction().spool_area().scratch_dir(),
                         kPrivateDirMode);
  CreateDirectoryAsOwner(settings_.transaction().spool_area().ovl_work_dir(),
                         kPrivateDirMode);

  // On a managed node, the mount points are already mounted
  if (!DirectoryExists(settings_.transaction().spool_area().readonly_mnt())) {
    CreateDirectoryAsOwner(settings_.transaction().spool_area().readonly_mnt(),
                           kPrivateDirMode);
  }
  if (!DirectoryExists(settings_.transaction().spool_area().union_mnt())) {
    CreateDirectoryAsOwner(
      settings_.transaction().spool_area().union_mnt(), kPrivateDirMode);
  }
}

Publisher::Publisher()
  : settings_("invalid.cvmfs.io")
  , statistics_publish_(new perf::StatisticsTemplate("publish", statistics_))
  , llvl_(kLogNormal)
  , in_transaction_(false)
  , spooler_files_(NULL)
  , spooler_catalogs_(NULL)
  , catalog_mgr_(NULL)
  , sync_parameters_(NULL)
  , sync_mediator_(NULL)
  , sync_union_(NULL)
{
}

Publisher::Publisher(const SettingsPublisher &settings)
  : Repository(SettingsRepository(settings))
  , settings_(settings)
  , statistics_publish_(new perf::StatisticsTemplate("publish", statistics_))
  , llvl_(settings.is_silent() ? kLogNone : kLogNormal)
  , in_transaction_(false)
  , spooler_files_(NULL)
  , spooler_catalogs_(NULL)
  , catalog_mgr_(NULL)
  , sync_parameters_(NULL)
  , sync_mediator_(NULL)
  , sync_union_(NULL)
{
  if (settings.transaction().layout_revision() != kRequiredLayoutRevision) {
    unsigned layout_revision = settings.transaction().layout_revision();
    throw EPublish(
      "This repository uses layout revision " + StringifyInt(layout_revision)
        + ".\n"
      "This version of CernVM-FS requires layout revision " + StringifyInt(
        kRequiredLayoutRevision) + ", which is\n"
      "incompatible to " + StringifyInt(layout_revision) + ".\n\n"
      "Please run `cvmfs_server migrate` to update your repository before "
      "proceeding.",
      EPublish::kFailLayoutRevision);
  }

  CreateDirectoryAsOwner(settings_.transaction().spool_area().tmp_dir(),
                         kPrivateDirMode);

  bool check_keys_match = true;
  int rvb;
  rvb =
    signature_mgr_->LoadCertificatePath(settings.keychain().certificate_path());
  if (!rvb) {
    check_keys_match = false;
    LogCvmfs(kLogCvmfs, kLogStdout | llvl_,
             "Warning: cannot load certificate, thus cannot commit changes");
  }
  rvb = signature_mgr_->LoadPrivateKeyPath(
    settings.keychain().private_key_path(), "");
  if (!rvb) {
    check_keys_match = false;
    LogCvmfs(kLogCvmfs, kLogStdout | llvl_,
             "Warning: cannot load private key, thus cannot commit changes");
  }
  // The private master key might be on a key card instead
  if (FileExists(settings.keychain().master_private_key_path())) {
    rvb = signature_mgr_->LoadPrivateMasterKeyPath(
      settings.keychain().master_private_key_path());
    if (!rvb) throw EPublish("cannot load private master key");
  }
  if (check_keys_match && !signature_mgr_->KeysMatch())
    throw EPublish("corrupted keychain");

  if (settings.storage().type() == upload::SpoolerDefinition::Gateway) {
    if (!settings.keychain().HasGatewayKey()) {
      throw EPublish("gateway key missing: " +
                     settings.keychain().gw_key_path());
    }
    gw_key_ = gateway::ReadGatewayKey(settings.keychain().gw_key_path());
    if (!gw_key_.IsValid()) {
      throw EPublish("cannot read gateway key: " +
                     settings.keychain().gw_key_path());
    }
  }

  if (settings.is_managed())
    managed_node_ = new ManagedNode(this);
  CheckTransactionStatus();
  if (in_transaction_)
    ConstructSpoolers();
}

Publisher::~Publisher() {
  delete sync_union_;
  delete sync_mediator_;
  delete sync_parameters_;
  delete catalog_mgr_;
  delete spooler_catalogs_;
  delete spooler_files_;
}


void Publisher::ConstructSyncManagers() {
  ConstructSpoolers();

  if (catalog_mgr_ == NULL) {
    catalog_mgr_ = new catalog::WritableCatalogManager(
      settings_.transaction().base_hash(),
      settings_.url(),
      settings_.transaction().spool_area().tmp_dir(),
      spooler_catalogs_,
      download_mgr_,
      settings_.transaction().enforce_limits(),
      settings_.transaction().limit_nested_catalog_kentries(),
      settings_.transaction().limit_root_catalog_kentries(),
      settings_.transaction().limit_file_size_mb(),
      statistics_,
      settings_.transaction().use_catalog_autobalance(),
      settings_.transaction().autobalance_max_weight(),
      settings_.transaction().autobalance_min_weight());
    catalog_mgr_->Init();
  }

  if (sync_parameters_ == NULL) {
    SyncParameters *p = new SyncParameters();
    p->spooler = spooler_files_;
    p->repo_name = settings_.fqrn();
    p->dir_union = settings_.transaction().spool_area().union_mnt();
    p->dir_scratch = settings_.transaction().spool_area().scratch_dir();
    p->dir_rdonly = settings_.transaction().spool_area().readonly_mnt();
    p->dir_temp = settings_.transaction().spool_area().tmp_dir();
    p->base_hash = settings_.transaction().base_hash();
    p->stratum0 = settings_.url();
    // p->manifest_path = SHOULD NOT BE NEEDED
    // p->spooler_definition = SHOULD NOT BE NEEDED;
    // p->union_fs_type = SHOULD NOT BE NEEDED
    p->print_changeset = settings_.transaction().print_changeset();
    p->dry_run = settings_.transaction().dry_run();
    sync_parameters_ = p;
  }

  if (sync_mediator_ == NULL) {
    sync_mediator_ =
      new SyncMediator(catalog_mgr_, sync_parameters_, *statistics_publish_);
  }

  if (sync_union_ == NULL) {
    switch (settings_.transaction().union_fs()) {
      case kUnionFsAufs:
        sync_union_ = new publish::SyncUnionAufs(
          sync_mediator_,
          settings_.transaction().spool_area().readonly_mnt(),
          settings_.transaction().spool_area().union_mnt(),
          settings_.transaction().spool_area().scratch_dir());
        break;
      case kUnionFsOverlay:
        sync_union_ = new publish::SyncUnionOverlayfs(
          sync_mediator_,
          settings_.transaction().spool_area().readonly_mnt(),
          settings_.transaction().spool_area().union_mnt(),
          settings_.transaction().spool_area().scratch_dir());
        break;
      case kUnionFsTarball:
        sync_union_ = new publish::SyncUnionTarball(
          sync_mediator_,
          settings_.transaction().spool_area().readonly_mnt(),
          // TODO(jblomer): get from settings
          "tar_file",
          "base_directory",
          "to_delete",
          false /* create_catalog */);
        break;
      default:
        throw EPublish("unknown union file system");
    }
    bool rvb = sync_union_->Initialize();
    if (!rvb) {
      delete sync_union_;
      sync_union_ = NULL;
      throw EPublish("cannot initialize union file system engine");
    }
  }
}

void Publisher::Sync() {
  if (settings_.transaction().dry_run()) {
    SyncImpl();
    return;
  }

  const std::string publishing_lock =
    settings_.transaction().spool_area().publishing_lock();

  ServerLockFile::Acquire(publishing_lock, false /* ignore_stale */);
  try {
    SyncImpl();
    ServerLockFile::Release(publishing_lock);
  } catch (...) {
    ServerLockFile::Release(publishing_lock);
    throw;
  }
}

void Publisher::SyncImpl() {
  ConstructSyncManagers();

  sync_union_->Traverse();
  bool rvb = sync_mediator_->Commit(manifest_);
  if (!rvb) throw EPublish("cannot write change set to storage");

  if (!settings_.transaction().dry_run()) {
    spooler_files_->WaitForUpload();
    spooler_catalogs_->WaitForUpload();
    spooler_files_->FinalizeSession(false /* commit */);

    const std::string old_root_hash =
      settings_.transaction().base_hash().ToString(true /* with_suffix */);
    const std::string new_root_hash =
      manifest_->catalog_hash().ToString(true /* with_suffix */);
    rvb = spooler_catalogs_->FinalizeSession(true /* commit */,
      old_root_hash, new_root_hash,
      /* TODO(jblomer) */ sync_parameters_->repo_tag);
    if (!rvb)
      throw EPublish("failed to commit transaction");

    // Reset to the new catalog root hash
    settings_.GetTransaction()->SetBaseHash(manifest_->catalog_hash());
    // TODO(jblomer): think about how to deal with the scratch area at
    // this point
    // WipeScratchArea();
  }

  delete sync_union_;
  delete sync_mediator_;
  delete sync_parameters_;
  delete catalog_mgr_;
  sync_union_ = NULL;
  sync_mediator_ = NULL;
  sync_parameters_ = NULL;
  catalog_mgr_ = NULL;

  if (!settings_.transaction().dry_run()) {
    LogCvmfs(kLogCvmfs, kLogStdout, "New revision: %d", manifest_->revision());
    reflog_->AddCatalog(manifest_->catalog_hash());
  }
}

void Publisher::Publish() {
  if (!in_transaction_) throw EPublish("cannot publish outside transaction");

  PushReflog();
  PushManifest();
  in_transaction_ = false;
}


void Publisher::MarkReplicatible(bool value) {
  ConstructSpoolers();

  if (value) {
    spooler_files_->Upload("/dev/null", "/.cvmfs_master_replica");
  } else {
    spooler_files_->RemoveAsync("/.cvmfs_master_replica");
  }
  spooler_files_->WaitForUpload();
  if (spooler_files_->GetNumberOfErrors() > 0)
    throw EPublish("cannot set replication mode");
}

void Publisher::Ingest() {}
void Publisher::Migrate() {}
void Publisher::Resign() {}
void Publisher::Rollback() {}
void Publisher::UpdateMetaInfo() {}


//------------------------------------------------------------------------------


Replica::Replica(const SettingsReplica &settings) {
}


Replica::~Replica() {
}

}  // namespace publish
