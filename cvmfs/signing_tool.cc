/**
 * This file is part of the CernVM File System
 */

#include "signing_tool.h"

#include <string>

#include "manifest.h"
#include "object_fetcher.h"
#include "reflog.h"
#include "server_tool.h"
#include "upload.h"
#include "util/exception.h"
#include "util/pointer.h"

namespace {

typedef HttpObjectFetcher<> ObjectFetcher;

}  // namespace

SigningTool::SigningTool(ServerTool *server_tool) : server_tool_(server_tool) {}

SigningTool::~SigningTool() {}

SigningTool::Result SigningTool::Run(
    const std::string &manifest_path, const std::string &repo_url,
    const std::string &spooler_definition, const std::string &temp_dir,
    const std::string &certificate, const std::string &priv_key,
    const std::string &repo_name, const std::string &pwd,
    const std::string &meta_info, const std::string &reflog_chksum_path,
    const std::string &proxy,
    const bool garbage_collectable, const bool bootstrap_shortcuts,
    const bool return_early, const std::vector<shash::Any> reflog_catalogs) {
  shash::Any reflog_hash;
  if (reflog_chksum_path != "") {
    if (!manifest::Reflog::ReadChecksum(reflog_chksum_path, &reflog_hash)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Could not read reflog checksum");
      return kReflogChecksumMissing;
    }
  }

  UniquePtr<upload::Spooler> spooler;
  UniquePtr<manifest::Manifest> manifest;

  if (!DirectoryExists(temp_dir)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "%s does not exist", temp_dir.c_str());
    return kError;
  }

  // prepare global manager modules
  const bool follow_redirects = false;
  if (!server_tool_->InitDownloadManager(follow_redirects, proxy) ||
      !server_tool_->InitSignatureManager("", certificate, priv_key)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to init repo connection");
    return kInitError;
  }

  // init the download helper
  ObjectFetcher object_fetcher(repo_name, repo_url, temp_dir,
                               server_tool_->download_manager(),
                               server_tool_->signature_manager());

  // Load Manifest
  manifest = manifest::Manifest::LoadFile(manifest_path);
  if (!manifest.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to parse manifest");
    return kError;
  }

  // reflog_chksum_path wasn't given, the reflog checksum can possibly be
  // obtained from the manifest
  if (reflog_chksum_path.empty()) {
    reflog_hash = manifest->reflog_hash();
  }

  // Connect to the spooler
  const upload::SpoolerDefinition sd(spooler_definition,
                                     manifest->GetHashAlgorithm());
  spooler = upload::Spooler::Construct(sd);
  if (!spooler.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to setup upload spooler");
    return kInitError;
  }

  UniquePtr<manifest::Reflog> reflog;
  if (!reflog_hash.IsNull()) {
    reflog = server_tool_->FetchReflog(&object_fetcher, repo_name, reflog_hash);
    if (!reflog.IsValid()) {
      LogCvmfs(kLogCvmfs, kLogStderr, "reflog missing");
      return kReflogMissing;
    }
  } else {
    LogCvmfs(kLogCvmfs, kLogVerboseMsg, "no reflog (ignoring)");
    if (spooler->Peek(".cvmfsreflog")) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "no reflog hash specified but reflog is present");
      return kError;
    }
  }

  // From here on things are potentially put in backend storage

  // Register callback for retrieving the certificate hash
  upload::Spooler::CallbackPtr callback =
      spooler->RegisterListener(&SigningTool::CertificateUploadCallback, this);

  // Safe certificate (and wait for the upload through a Future)
  spooler->ProcessCertificate(certificate);
  const shash::Any certificate_hash = certificate_hash_.Get();
  spooler->UnregisterListener(callback);

  if (certificate_hash.IsNull()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to upload certificate");
    return kError;
  }

  // Safe repository meta info file
  shash::Any metainfo_hash = manifest->meta_info();
  if (!meta_info.empty()) {
    upload::Spooler::CallbackPtr callback =
        spooler->RegisterListener(&SigningTool::MetainfoUploadCallback, this);
    spooler->ProcessMetainfo(meta_info);
    metainfo_hash = metainfo_hash_.Get();
    spooler->UnregisterListener(callback);

    if (metainfo_hash.IsNull()) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to upload meta info");
      return kError;
    }
  }

  // Update Reflog database
  if (reflog.IsValid()) {
    reflog->BeginTransaction();

    if (!reflog->AddCatalog(manifest->catalog_hash())) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to add catalog to Reflog");
      return kError;
    }

    if (!reflog->AddCertificate(certificate_hash)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to add certificate to Reflog");
      return kError;
    }

    if (!manifest->history().IsNull()) {
      if (!reflog->AddHistory(manifest->history())) {
        LogCvmfs(kLogCvmfs, kLogStderr, "Failed to add history to Reflog");
        return kError;
      }
    }

    if (!metainfo_hash.IsNull()) {
      if (!reflog->AddMetainfo(metainfo_hash)) {
        LogCvmfs(kLogCvmfs, kLogStderr, "Failed to add meta info to Reflog");
        return kError;
      }
    }

    // Callers of SigningTool may provide a list of additional catalogs that
    // need to be added to reflog (e. g. for later garbage collection)
    std::vector<shash::Any>::const_iterator i = reflog_catalogs.begin();
    std::vector<shash::Any>::const_iterator iend = reflog_catalogs.end();
    for (; i != iend; ++i) {
      if (!reflog->AddCatalog(*i)) {
        LogCvmfs(kLogCvmfs, kLogStderr,
                 "Failed to add additional catalog %s to Reflog",
                 (*i).ToString().c_str());
        return kError;
      }
    }

    reflog->CommitTransaction();

    // upload Reflog database
    reflog->DropDatabaseFileOwnership();
    const std::string reflog_db_file = reflog->database_file();
    reflog.Destroy();
    spooler->UploadReflog(reflog_db_file);
    spooler->WaitForUpload();
    reflog_hash.algorithm = manifest->GetHashAlgorithm();
    manifest::Reflog::HashDatabase(reflog_db_file, &reflog_hash);
    unlink(reflog_db_file.c_str());
    if (spooler->GetNumberOfErrors()) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to upload Reflog (errors: %d)",
               spooler->GetNumberOfErrors());
      return kError;
    }
    if (!reflog_chksum_path.empty())
      manifest::Reflog::WriteChecksum(reflog_chksum_path, reflog_hash);
  }

  // Don't activate new manifest, just make sure all its references are uploaded
  // and entered into the reflog
  if (return_early) {
    return kSuccess;
  }

  // Update manifest
  manifest->set_certificate(certificate_hash);
  manifest->set_repository_name(repo_name);
  manifest->set_publish_timestamp(time(NULL));
  manifest->set_garbage_collectability(garbage_collectable);
  manifest->set_has_alt_catalog_path(bootstrap_shortcuts);
  if (!metainfo_hash.IsNull()) {
    manifest->set_meta_info(metainfo_hash);
  }
  if (!reflog_hash.IsNull()) {
    manifest->set_reflog_hash(reflog_hash);
  }

  std::string signed_manifest = manifest->ExportString();
  shash::Any published_hash(manifest->GetHashAlgorithm());
  shash::HashMem(
      reinterpret_cast<const unsigned char *>(signed_manifest.data()),
      signed_manifest.length(), &published_hash);
  signed_manifest += "--\n" + published_hash.ToString() + "\n";

  // Create alternative bootstrapping symlinks for VOMS secured repos
  if (manifest->has_alt_catalog_path()) {
    const bool success =
        spooler->PlaceBootstrappingShortcut(manifest->certificate()) &&
        spooler->PlaceBootstrappingShortcut(manifest->catalog_hash()) &&
        (manifest->history().IsNull() ||
         spooler->PlaceBootstrappingShortcut(manifest->history())) &&
        (metainfo_hash.IsNull() ||
         spooler->PlaceBootstrappingShortcut(metainfo_hash));

    if (!success) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "failed to place VOMS bootstrapping "
               "symlinks");
      return kError;
    }
  }

  // Sign manifest
  unsigned char *sig;
  unsigned sig_size;
  const bool manifest_was_signed = server_tool_->signature_manager()->Sign(
      reinterpret_cast<const unsigned char *>(published_hash.ToString().data()),
      published_hash.GetHexSize(), &sig, &sig_size);
  if (!manifest_was_signed) {
    PANIC(kLogStderr, "Failed to sign manifest");
  }

  // Write new manifest
  signed_manifest += std::string(reinterpret_cast<char *>(sig), sig_size);
  free(sig);
  if (!SafeWriteToFile(signed_manifest, manifest_path, 0664)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to write manifest (errno: %d)",
             errno);
    return kError;
  }

  // Upload manifest
  spooler->UploadManifest(manifest_path);
  spooler->WaitForUpload();
  unlink(manifest_path.c_str());
  if (spooler->GetNumberOfErrors()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to commit manifest (errors: %d)",
             spooler->GetNumberOfErrors());
    return kError;
  }

  return kSuccess;
}

void SigningTool::CertificateUploadCallback(
    const upload::SpoolerResult &result) {
  shash::Any certificate_hash;
  if (result.return_code == 0) {
    certificate_hash = result.content_hash;
  } else {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Failed to upload certificate "
             "(retcode: %d)",
             result.return_code);
  }
  certificate_hash_.Set(certificate_hash);
}

void SigningTool::MetainfoUploadCallback(const upload::SpoolerResult &result) {
  shash::Any metainfo_hash;
  if (result.return_code == 0) {
    metainfo_hash = result.content_hash;
  } else {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to upload meta info (retcode: %d)",
             result.return_code);
  }
  metainfo_hash_.Set(metainfo_hash);
}
