/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "publish/repository.h"

#include <cstddef>

#include "logging.h"
#include "publish/settings.h"
#include "publish/except.h"
#include "signature.h"
#include "upload.h"
#include "upload_spooler_definition.h"
#include "util/pointer.h"
#include "whitelist.h"

namespace publish {

Repository::Repository()
  : spooler_(NULL)
  , whitelist_(NULL)
{ }

Repository::~Repository() {
  delete whitelist_;
  delete spooler_;
}


//------------------------------------------------------------------------------


Publisher *Publisher::Create(const SettingsPublisher &settings) {
  UniquePtr<Publisher> publisher(new Publisher(settings));

  LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "Creating Key Chain... ");
  if (settings.keychain().HasDanglingMasterKeys()) {
    throw EPublish("dangling master key pair");
  }
  if (settings.keychain().HasDanglingRepositoryKeys()) {
    throw EPublish("dangling repository keys");
  }
  if (!settings.keychain().HasMasterKeys())
    publisher->signature_mgr()->GenerateMasterKeyPair();
  if (!settings.keychain().HasRepositoryKeys())
    publisher->signature_mgr()->GenerateCertificate(settings.fqrn());
  LogCvmfs(kLogCvmfs, kLogStdout, "done");

  upload::SpoolerDefinition sd(
    settings.storage().GetLocator(),
    settings.transaction().hash_algorithm(),
    settings.transaction().compression_algorithm());
  UniquePtr<upload::Spooler> spooler(upload::Spooler::Construct(sd));
  if (!spooler.IsValid()) throw EPublish("could not initialize spooler");

  LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
           "Creating Backend Storage... ");
  if (!spooler->Create())
    throw EPublish("could not initialize repository storage area");
  publisher->TakeSpooler(spooler.Release());
  LogCvmfs(kLogCvmfs, kLogStdout, "done");

  LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
           "Creating Initial Repository... ");
  UniquePtr<whitelist::Whitelist> whitelist(new whitelist::Whitelist(
    settings.fqrn(), NULL, publisher->signature_mgr()));
  assert(whitelist.IsValid());
  whitelist::Failures rv_wl = whitelist->LoadMem(whitelist::Whitelist::Create(
    settings.fqrn(),
    settings.whitelist_validity_days(),
    settings.transaction().hash_algorithm(),
    publisher->signature_mgr()));
  if (rv_wl != whitelist::kFailOk)
    throw EPublish("whitelist generation failed");
  publisher->TakeWhitelist(whitelist.Release());
  // Upload whitelist (+ PKCS7) and certificate
  // Create root catalog, upload it together with manifest, reflog
  LogCvmfs(kLogCvmfs, kLogStdout, "done");

  return publisher.Release();
}

Publisher::Publisher(const SettingsPublisher &settings)
  : settings_(settings)
  , signature_mgr_(new signature::SignatureManager())
{
  signature_mgr_->Init();
}

Publisher::~Publisher() {
  signature_mgr_->Fini();
  delete signature_mgr_;
}

void Publisher::EditTags() {}
void Publisher::Ingest() {}
void Publisher::Migrate() {}
void Publisher::Resign() {}
void Publisher::Rollback() {}
void Publisher::UpdateMetaInfo() {}

}
