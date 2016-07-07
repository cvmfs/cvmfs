/**
 * This file is part of the CernVM File System
 *
 * This tool signs a CernVM-FS manifest with an X.509 certificate.
 */

#include "cvmfs_config.h"
#include "swissknife_sign.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <termios.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <iostream>  // TODO(jblomer): remove
#include <set>
#include <string>
#include <vector>

#include "compression.h"
#include "hash.h"
#include "logging.h"
#include "manifest.h"
#include "object_fetcher.h"
#include "reflog.h"
#include "signature.h"
#include "smalloc.h"
#include "upload.h"
#include "util/posix.h"

using namespace std;  // NOLINT

typedef HttpObjectFetcher<> ObjectFetcher;

int swissknife::CommandSign::Main(const swissknife::ArgumentList &args) {
  string manifest_path = *args.find('m')->second;
  string repo_url = *args.find('u')->second;
  string spooler_definition = *args.find('r')->second;
  string temp_dir = *args.find('t')->second;

  string certificate = "";
  if (args.find('c') != args.end()) certificate = *args.find('c')->second;
  string priv_key = "";
  if (args.find('k') != args.end()) priv_key = *args.find('k')->second;
  string repo_name = "";
  if (args.find('n') != args.end()) repo_name = *args.find('n')->second;
  string pwd = "";
  if (args.find('s') != args.end()) pwd = *args.find('s')->second;
  string meta_info = "";
  if (args.find('M') != args.end()) meta_info = *args.find('M')->second;
  const bool garbage_collectable = (args.count('g') > 0);
  const bool bootstrap_shortcuts = (args.count('A') > 0);

  UniquePtr<upload::Spooler>    spooler;
  UniquePtr<manifest::Manifest> manifest;

  if (!DirectoryExists(temp_dir)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "%s does not exist", temp_dir.c_str());
    return 1;
  }

  // prepare global manager modules
  const bool follow_redirects = false;
  if (!this->InitDownloadManager(follow_redirects) ||
      !this->InitSigningSignatureManager(certificate, priv_key, pwd)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to init repo connection");
    return 2;
  }

  // init the download helper
  ObjectFetcher object_fetcher(repo_name,
                               repo_url,
                               temp_dir,
                               download_manager(),
                               signature_manager());

  // Load Manifest
  manifest = manifest::Manifest::LoadFile(manifest_path);
  if (!manifest.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to parse manifest");
    return 1;
  }

  // Connect to the spooler
  const upload::SpoolerDefinition sd(spooler_definition,
                                     manifest->GetHashAlgorithm());
  spooler = upload::Spooler::Construct(sd);
  if (!spooler.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to setup upload spooler");
    return 1;
  }

  UniquePtr<manifest::Reflog> reflog;
  reflog = FetchReflog(&object_fetcher, repo_name);
  if (!reflog) {
    LogCvmfs(kLogCvmfs, kLogVerboseMsg, "failed to fetch Reflog (ignoring)");
  }

  // Fron here on things are potentially put in backend storage
  LogCvmfs(kLogCvmfs, kLogStdout, "Signing %s", manifest_path.c_str());

  // Register callback for retrieving the certificate hash
  upload::Spooler::CallbackPtr callback =
    spooler->RegisterListener(&CommandSign::CertificateUploadCallback, this);

  // Safe certificate (and wait for the upload through a Future)
  spooler->ProcessCertificate(certificate);
  const shash::Any certificate_hash = certificate_hash_.Get();
  spooler->UnregisterListener(callback);

  if (certificate_hash.IsNull()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to upload certificate");
    return 1;
  }

  // Safe repository meta info file
  shash::Any metainfo_hash;
  if (!meta_info.empty()) {
    upload::Spooler::CallbackPtr callback =
      spooler->RegisterListener(&CommandSign::MetainfoUploadCallback, this);
    spooler->ProcessMetainfo(meta_info);
    metainfo_hash = metainfo_hash_.Get();
    spooler->UnregisterListener(callback);

    if (metainfo_hash.IsNull()) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to upload meta info");
      return 1;
    }
  }

  // Update Reflog database
  if (reflog.IsValid()) {
    reflog->BeginTransaction();

    if (!reflog->AddCatalog(manifest->catalog_hash())) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to add catalog to Reflog");
      return 1;
    }

    if (!reflog->AddCertificate(certificate_hash)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to add certificate to Reflog");
      return 1;
    }

    if (!manifest->history().IsNull()) {
      if (!reflog->AddHistory(manifest->history())) {
        LogCvmfs(kLogCvmfs, kLogStderr, "Failed to add history to Reflog");
        return 1;
      }
    }

    if (!metainfo_hash.IsNull()) {
      if (!reflog->AddMetainfo(metainfo_hash)) {
        LogCvmfs(kLogCvmfs, kLogStderr, "Failed to add meta info to Reflog");
        return 1;
      }
    }

    reflog->CommitTransaction();

    // upload Reflog database
    const std::string reflog_db_file = reflog->CloseAndReturnDatabaseFile();
    spooler->UploadReflog(reflog_db_file);
    spooler->WaitForUpload();
    unlink(reflog_db_file.c_str());
    if (spooler->GetNumberOfErrors()) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to upload Reflog (errors: %d)",
               spooler->GetNumberOfErrors());
      return 1;
    }
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

  string signed_manifest = manifest->ExportString();
  shash::Any published_hash(manifest->GetHashAlgorithm());
  shash::HashMem(
    reinterpret_cast<const unsigned char *>(signed_manifest.data()),
    signed_manifest.length(), &published_hash);
  signed_manifest += "--\n" + published_hash.ToString() + "\n";

  // Create alternative bootstrapping symlinks for VOMS secured repos
  if (manifest->has_alt_catalog_path()) {
    const bool success =
      spooler->PlaceBootstrappingShortcut(manifest->certificate())  &&
      spooler->PlaceBootstrappingShortcut(manifest->catalog_hash()) &&
      (manifest->history().IsNull() ||
       spooler->PlaceBootstrappingShortcut(manifest->history())) &&
      (metainfo_hash.IsNull() ||
       spooler->PlaceBootstrappingShortcut(metainfo_hash));

    if (!success) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to place VOMS bootstrapping "
                                      "symlinks");
      return 1;
    }
  }

  // Sign manifest
  unsigned char *sig;
  unsigned sig_size;
  if (!signature_manager()->Sign(reinterpret_cast<const unsigned char *>(
                                 published_hash.ToString().data()),
                                 published_hash.GetHexSize(),
                                 &sig, &sig_size))
  {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to sign manifest");
    return 1;
  }

  // Write new manifest
  signed_manifest += string(reinterpret_cast<char *>(sig), sig_size);
  free(sig);
  if (!SafeWriteToFile(signed_manifest, manifest_path, 0664)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to write manifest (errno: %d)",
             errno);
    return 1;
  }

  // Upload manifest
  spooler->UploadManifest(manifest_path);
  spooler->WaitForUpload();
  unlink(manifest_path.c_str());
  if (spooler->GetNumberOfErrors()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to commit manifest (errors: %d)",
             spooler->GetNumberOfErrors());
    return 1;
  }

  return 0;
}


void swissknife::CommandSign::CertificateUploadCallback(
                                          const upload::SpoolerResult &result) {
  shash::Any certificate_hash;
  if (result.return_code == 0) {
    certificate_hash = result.content_hash;
  } else {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to upload certificate "
                                    "(retcode: %d)",
                                    result.return_code);
  }
  certificate_hash_.Set(certificate_hash);
}


void swissknife::CommandSign::MetainfoUploadCallback(
                                          const upload::SpoolerResult &result) {
  shash::Any metainfo_hash;
  if (result.return_code == 0) {
    metainfo_hash = result.content_hash;
  } else {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to upload meta info (retcode: %d)",
             result.return_code);
  }
  metainfo_hash_.Set(metainfo_hash);
}
