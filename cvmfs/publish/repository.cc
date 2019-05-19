/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "publish/repository.h"

#include <cassert>
#include <cstddef>
#include <cstdlib>

#include "catalog_mgr_rw.h"
#include "download.h"
#include "hash.h"
#include "history_sqlite.h"
#include "ingestion/ingestion_source.h"
#include "logging.h"
#include "manifest.h"
#include "manifest_fetch.h"
#include "publish/settings.h"
#include "publish/except.h"
#include "reflog.h"
#include "signature.h"
#include "statistics.h"
#include "sync_mediator.h"
#include "sync_union_overlayfs.h"
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
  : statistics_(new perf::Statistics())
  , signature_mgr_(new signature::SignatureManager())
  , download_mgr_(NULL)
  , spooler_(NULL)
  , whitelist_(NULL)
  , reflog_(NULL)
  , manifest_(NULL)
  , history_(NULL)
{
  signature_mgr_->Init();
}

Repository::~Repository() {
  if (signature_mgr_ != NULL) signature_mgr_->Fini();
  if (download_mgr_ != NULL) download_mgr_->Fini();

  delete history_;
  delete manifest_;
  delete reflog_;
  delete whitelist_;
  delete spooler_;
  delete signature_mgr_;
  delete download_mgr_;
  delete statistics_;
}

history::History *Repository::history() { return history_; }

void Repository::DownloadRootObjects(
  const std::string &url, const std::string &fqrn, const std::string &tmp_dir)
{
  delete whitelist_;
  whitelist_ = new whitelist::Whitelist(fqrn, download_mgr_, signature_mgr_);
  whitelist_->LoadUrl(url);
  if (whitelist_->status() != whitelist::Whitelist::kStAvailable)
    throw EPublish("cannot load whitelist");

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
  //shash::Any reflog_hash(manifest_->GetHashAlgorithm());
  download::JobInfo download_reflog(
       &reflog_url,
       false /* compressed */,
       false /* probe hosts */,
       reflog_fd,
       NULL);
  download::Failures rv_dl = download_mgr_->Fetch(&download_reflog);
  fclose(reflog_fd);
  if (rv_dl != download::kFailOk) throw EPublish("cannot load reflog");
  delete reflog_;
  reflog_ = manifest::Reflog::Open(reflog_path);
  if (reflog_ == NULL) throw EPublish("cannot open reflog");

  std::string tags_path;
  FILE *tags_fd =
    CreateTempFile(tmp_dir + "/tags", kPrivateFileMode, "w", &tags_path);
  std::string tags_url = url + "/data/" + manifest_->history().MakePath();
  shash::Any tags_hash(manifest_->history());
  download::JobInfo download_tags(
       &tags_url,
       true /* compressed */,
       false /* probe hosts */,
       tags_fd,
       &tags_hash);
  rv_dl = download_mgr_->Fetch(&download_tags);
  fclose(tags_fd);
  if (rv_dl != download::kFailOk) throw EPublish("cannot load tag database");
  delete history_;
  history_ = history::SqliteHistory::OpenWritable(tags_path);
  if (history_ == NULL) throw EPublish("cannot open tag database");
}


//------------------------------------------------------------------------------


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
    spooler_);
  spooler_->WaitForUpload();
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
  upload::SpoolerDefinition sd(
    settings_.storage().GetLocator(),
    settings_.transaction().hash_algorithm(),
    settings_.transaction().compression_algorithm());
  spooler_ = upload::Spooler::Construct(sd);
  if (spooler_ == NULL) throw EPublish("could not initialize spooler");
  if (!spooler_->Create())
    throw EPublish("could not initialize repository storage area");
}


void Publisher::PushCertificate() {
  upload::Spooler::CallbackPtr callback =
    spooler_->RegisterListener(&Publisher::OnProcessCertificate, this);
  spooler_->ProcessCertificate(
    new StringIngestionSource(signature_mgr_->GetCertificate()));
  spooler_->WaitForUpload();
  spooler_->UnregisterListener(callback);
}


void Publisher::PushHistory() {
  assert(history_ != NULL);
  history_->SetPreviousRevision(manifest_->history());
  const string history_path = history_->filename();
  history_->DropDatabaseFileOwnership();
  delete history_;

  upload::Spooler::CallbackPtr callback =
    spooler_->RegisterListener(&Publisher::OnProcessHistory, this);
  spooler_->ProcessHistory(history_path);
  spooler_->WaitForUpload();
  spooler_->UnregisterListener(callback);

  history_ = history::SqliteHistory::OpenWritable(history_path);
  assert(history_ != NULL);
  history_->TakeDatabaseFileOwnership();
}


void Publisher::PushMetainfo() {
  upload::Spooler::CallbackPtr callback =
    spooler_->RegisterListener(&Publisher::OnProcessMetainfo, this);
  spooler_->ProcessMetainfo(new StringIngestionSource(meta_info_));
  spooler_->WaitForUpload();
  spooler_->UnregisterListener(callback);
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
    rvb = spooler_->PlaceBootstrappingShortcut(manifest_->certificate()) &&
          spooler_->PlaceBootstrappingShortcut(manifest_->catalog_hash()) &&
          (manifest_->history().IsNull() ||
            spooler_->PlaceBootstrappingShortcut(manifest_->history())) &&
          (manifest_->meta_info().IsNull() ||
            spooler_->PlaceBootstrappingShortcut(manifest_->meta_info()));
    if (!rvb) EPublish("cannot place VOMS bootstrapping symlinks");
  }

  upload::Spooler::CallbackPtr callback =
    spooler_->RegisterListener(&Publisher::OnUploadManifest, this);
  spooler_->Upload(".cvmfspublished",
                   new StringIngestionSource(signed_manifest));
  spooler_->WaitForUpload();
  spooler_->UnregisterListener(callback);
}


void Publisher::PushReflog() {
  const string reflog_path = reflog_->database_file();
  reflog_->DropDatabaseFileOwnership();
  delete reflog_;

  shash::Any hash_reflog(settings_.transaction().hash_algorithm());
  manifest::Reflog::HashDatabase(reflog_path, &hash_reflog);

  upload::Spooler::CallbackPtr callback =
    spooler_->RegisterListener(&Publisher::OnUploadReflog, this);
  spooler_->UploadReflog(reflog_path);
  spooler_->WaitForUpload();
  spooler_->UnregisterListener(callback);

  manifest_->set_reflog_hash(hash_reflog);

  reflog_ = manifest::Reflog::Open(reflog_path);
  assert(reflog_ != NULL);
  reflog_->TakeDatabaseFileOwnership();
}


void Publisher::PushWhitelist() {
  // TODO(jblomer): PKCS7 handling
  upload::Spooler::CallbackPtr callback =
    spooler_->RegisterListener(&Publisher::OnUploadWhitelist, this);
  spooler_->Upload(".cvmfswhitelist",
                   new StringIngestionSource(whitelist_->ExportString()));
  spooler_->WaitForUpload();
  spooler_->UnregisterListener(callback);
}


Publisher *Publisher::Create(const SettingsPublisher &settings) {
  UniquePtr<Publisher> publisher(new Publisher());
  publisher->settings_ = settings;
  publisher->signature_mgr_ = new signature::SignatureManager();
  publisher->signature_mgr_->Init();

  LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "Creating Key Chain... ");
  publisher->CreateKeychain();
  publisher->ExportKeychain();
  LogCvmfs(kLogCvmfs, kLogStdout, "done");

  LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
           "Creating Backend Storage... ");
  publisher->CreateStorage();
  publisher->PushWhitelist();
  LogCvmfs(kLogCvmfs, kLogStdout, "done");

  LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
           "Creating Initial Repository... ");
  publisher->CreateRootObjects();
  publisher->PushHistory();
  publisher->PushCertificate();
  publisher->PushMetainfo();
  publisher->PushReflog();
  publisher->PushManifest();
  // TODO: meta-info
  LogCvmfs(kLogCvmfs, kLogStdout, "done");

  return publisher.Release();
}

void Publisher::ExportKeychain() {
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
  CreateDirectoryAsOwner(settings_.transaction().spool_area().readonly_mnt(),
                         kPrivateDirMode);
  CreateDirectoryAsOwner(
    settings_.transaction().spool_area().union_mnt() + "/" + settings_.fqrn(),
    kPrivateDirMode);
}

Publisher::Publisher()
  : settings_("invalid.cvmfs.io")
{
}

Publisher::Publisher(const SettingsPublisher &settings)
  : settings_(settings)
{
  CreateDirectoryAsOwner(settings_.transaction().spool_area().tmp_dir(),
                         kPrivateDirMode);

  // TODO(jblomer): use key directory if applicable
  int rvb;
  rvb = signature_mgr_->LoadPublicRsaKeys(
    settings.keychain().master_public_key_path());
  if (!rvb) throw EPublish("cannot load public rsa key");
  rvb =
    signature_mgr_->LoadCertificatePath(settings.keychain().certificate_path());
  if (!rvb) throw EPublish("cannot load certificate");
  rvb = signature_mgr_->LoadPrivateKeyPath(
    settings.keychain().private_key_path(), "");
  if (!rvb) throw EPublish("cannot load private key");
  // TODO(jblomer): make optional
  rvb = signature_mgr_->LoadPrivateMasterKeyPath(
    settings.keychain().master_private_key_path());
  if (!rvb) throw EPublish("cannot load private master key");
  if (!signature_mgr_->KeysMatch()) throw EPublish("corrupted keychain");

  download_mgr_ = new download::DownloadManager();
  download_mgr_->Init(16, false,
                      perf::StatisticsTemplate("download", statistics_));
  DownloadRootObjects(settings.url(), settings.fqrn(),
                      settings.transaction().spool_area().tmp_dir());

  // TODO(jblomer): check transaction lock
}

Publisher::~Publisher() {
}

void Publisher::Transaction() {
  InitSpoolArea();
  // TODO(jblomer): set transaction lock
}
void Publisher::Abort() {
  // TODO(jblomer): remove transaction lock
}
void Publisher::Publish() {
  LogCvmfs(kLogCvmfs, kLogStdout, "Staet at revision: %d",
           manifest_->revision());
  upload::SpoolerDefinition sd(
    settings_.storage().GetLocator(),
    settings_.transaction().hash_algorithm(),
    settings_.transaction().compression_algorithm());
  spooler_ = upload::Spooler::Construct(sd);
  if (spooler_ == NULL) throw EPublish("could not initialize spooler");

  catalog::WritableCatalogManager catalog_mgr(
    manifest_->catalog_hash(),
    settings_.url(),
    settings_.transaction().spool_area().tmp_dir(),
    spooler_,
    download_mgr_,
    false /* enforce limits */,
    100000,
    100000,
    1000,
    statistics_,
    false /* auto balance */,
    1000,
    100000
  );
  catalog_mgr.Init();

  SyncParameters params;
  params.spooler = spooler_;
  params.repo_name = settings_.fqrn();
  params.dir_union = std::string("/cvmfs/") + settings_.fqrn();
  params.dir_scratch = settings_.transaction().spool_area().scratch_dir();
  params.dir_rdonly = settings_.transaction().spool_area().readonly_mnt();
  params.dir_temp = settings_.transaction().spool_area().tmp_dir();
  params.base_hash = manifest_->catalog_hash();
  params.stratum0 = settings_.url();
  //params.manifest_path = SHOULD NOT BE NEEDED
  // params.spooler_definition = SHOULD NOT BE NEEDED;
  params.union_fs_type = "overlayfs";  // TODO
  params.print_changeset = true;
  SyncMediator mediator(&catalog_mgr, &params,
                        perf::StatisticsTemplate("Publish", statistics_));
  publish::SyncUnion *sync;
  sync = new publish::SyncUnionOverlayfs(&mediator,
    settings_.transaction().spool_area().readonly_mnt(),
    std::string("/cvmfs/") + settings_.fqrn(),
    settings_.transaction().spool_area().scratch_dir());
  bool rvb = sync->Initialize();
  if (!rvb) throw EPublish("cannot initialize union file system engine");
  sync->Traverse();
  rvb = mediator.Commit(manifest_);
  if (!rvb) throw EPublish("cannot write change set to storage");
  spooler_->WaitForUpload();

  LogCvmfs(kLogCvmfs, kLogStdout, "New revision: %d", manifest_->revision());
  reflog_->AddCatalog(manifest_->catalog_hash());
  PushManifest();
  PushReflog();
  // TODO(jblomer): check transaction lock and remove if successful
}
void Publisher::EditTags() {}
void Publisher::Ingest() {}
void Publisher::Migrate() {}
void Publisher::Resign() {}
void Publisher::Rollback() {}
void Publisher::UpdateMetaInfo() {}


//------------------------------------------------------------------------------


Replica::~Replica() {

}

}  // namespace publish
