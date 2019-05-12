/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "publish/repository.h"

#include <cassert>
#include <cstddef>
#include <cstdlib>

#include "catalog_mgr_rw.h"
#include "hash.h"
#include "history_sqlite.h"
#include "ingestion/ingestion_source.h"
#include "logging.h"
#include "manifest.h"
#include "publish/settings.h"
#include "publish/except.h"
#include "reflog.h"
#include "signature.h"
#include "upload.h"
#include "upload_spooler_definition.h"
#include "util/pointer.h"
#include "whitelist.h"

namespace publish {

Repository::Repository()
  : spooler_(NULL)
  , whitelist_(NULL)
  , reflog_(NULL)
  , manifest_(NULL)
  , history_(NULL)
{ }

Repository::~Repository() {
  delete history_;
  delete manifest_;
  delete reflog_;
  delete whitelist_;
  delete spooler_;
}

history::History *Repository::history() { return history_; }


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

  UniquePtr<whitelist::Whitelist> whitelist(new whitelist::Whitelist(
    settings_.fqrn(), NULL, signature_mgr_));
  assert(whitelist.IsValid());
  std::string whitelist_str = whitelist::Whitelist::CreateString(
    settings_.fqrn(),
    settings_.whitelist_validity_days(),
    settings_.transaction().hash_algorithm(),
    signature_mgr_);
  whitelist::Failures rv_wl = whitelist->LoadMem(whitelist_str);
  if (rv_wl != whitelist::kFailOk)
    throw EPublish("whitelist generation failed");
  TakeWhitelist(whitelist.Release());
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
  UniquePtr<upload::Spooler> spooler(upload::Spooler::Construct(sd));
  if (!spooler.IsValid()) throw EPublish("could not initialize spooler");
  if (!spooler->Create())
    throw EPublish("could not initialize repository storage area");
  TakeSpooler(spooler.Release());
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

  upload::Spooler::CallbackPtr callback =
    spooler_->RegisterListener(&Publisher::OnUploadReflog, this);
  spooler_->UploadReflog(reflog_path);
  spooler_->WaitForUpload();
  spooler_->UnregisterListener(callback);

  shash::Any hash_reflog(settings_.transaction().hash_algorithm());
  manifest::Reflog::HashDatabase(reflog_path, &hash_reflog);
  //TODO(jblomer) set hash in manifest

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
  , signature_mgr_(NULL)
{
}

Publisher::Publisher(const SettingsPublisher &settings)
  : settings_(settings)
  , signature_mgr_(new signature::SignatureManager())
{
  signature_mgr_->Init();
  // TODO(jblomer): check transaction lock
}

Publisher::~Publisher() {
  signature_mgr_->Fini();
  delete signature_mgr_;
}

void Publisher::Transaction() {
  InitSpoolArea();
  // TODO(jblomer): set transaction lock
}
void Publisher::Abort() {
  // TODO(jblomer): remove transaction lock
}
void Publisher::Publish() {
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
