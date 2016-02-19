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
#include "signature.h"
#include "smalloc.h"
#include "upload.h"
#include "util.h"

using namespace std;  // NOLINT


int swissknife::CommandSign::Main(const swissknife::ArgumentList &args) {
  string manifest_path = *args.find('m')->second;
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
  upload::Spooler *spooler = NULL;
  const bool garbage_collectable = (args.count('g') > 0);
  const bool bootstrap_shortcuts = (args.count('A') > 0);

  if (!DirectoryExists(temp_dir)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "%s does not exist", temp_dir.c_str());
    return 1;
  }

  signature::SignatureManager signature_manager;
  signature_manager.Init();

  // Load certificate
  unsigned char *cert_buf = NULL;
  unsigned cert_buf_size;
  if (certificate == "") {
    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
             "Enter file name of X509 certificate []: ");
    getline(cin, certificate);
  }
  if (!signature_manager.LoadCertificatePath(certificate) ||
      !signature_manager.WriteCertificateMem(&cert_buf, &cert_buf_size))
  {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load certificate");
    return 2;
  }

  // Load private key
  // TODO(rmeusel): eliminiate code duplication with swissknife_letter.cc
  if (priv_key == "") {
    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
             "Enter file name of private key file to your certificate []: ");
    getline(cin, priv_key);
  }
  if (!signature_manager.LoadPrivateKeyPath(priv_key, pwd)) {
    int retry = 0;
    bool success;
    do {
      struct termios defrsett, newrsett;
      char c;
      tcgetattr(fileno(stdin), &defrsett);
      newrsett = defrsett;
      newrsett.c_lflag &= ~ECHO;
      if (tcsetattr(fileno(stdin), TCSAFLUSH, &newrsett) != 0) {
        LogCvmfs(kLogCvmfs, kLogStderr, "terminal failure");
        free(cert_buf);
        return 2;
      }

      LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
               "Enter password for private key: ");
      pwd = "";
      while (cin.get(c) && (c != '\n'))
        pwd += c;
      tcsetattr(fileno(stdin), TCSANOW, &defrsett);
      LogCvmfs(kLogCvmfs, kLogStdout, "");

      success = signature_manager.LoadPrivateKeyPath(priv_key, pwd);
      if (!success) {
        LogCvmfs(kLogCvmfs, kLogStderr, "failed to load private key (%s)",
                 signature_manager.GetCryptoError().c_str());
      }
      retry++;
    } while (!success && (retry < 3));
    if (!success) {
      free(cert_buf);
      return 2;
    }
  }
  if (!signature_manager.KeysMatch()) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "the private key doesn't seem to match your certificate (%s)",
             signature_manager.GetCryptoError().c_str());
    signature_manager.UnloadPrivateKey();
    free(cert_buf);
    return 2;
  }


  LogCvmfs(kLogCvmfs, kLogStdout, "Signing %s", manifest_path.c_str());
  {
    // Load Manifest
    // TODO(rmeusel): UniquePtr
    manifest::Manifest *manifest = manifest::Manifest::LoadFile(manifest_path);
    if (!manifest) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to parse manifest");
      goto sign_fail;
    }

    // Connect to the spooler
    const upload::SpoolerDefinition sd(spooler_definition,
                                       manifest->GetHashAlgorithm());
    spooler = upload::Spooler::Construct(sd);

    // Register callback for retrieving the certificate hash
    upload::Spooler::CallbackPtr callback =
      spooler->RegisterListener(&CommandSign::CertificateUploadCallback, this);

    // Safe certificate (and wait for the upload through a Future)
    spooler->ProcessCertificate(certificate);
    const shash::Any certificate_hash = certificate_hash_.Get();
    spooler->UnregisterListener(callback);

    if (certificate_hash.IsNull()) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to upload certificate");
      delete manifest;
      goto sign_fail;
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
        goto sign_fail;
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
        delete manifest;
        goto sign_fail;
      }
    }

    // Sign manifest
    unsigned char *sig;
    unsigned sig_size;
    if (!signature_manager.Sign(reinterpret_cast<const unsigned char *>(
                                published_hash.ToString().data()),
                                published_hash.GetHexSize(),
                                &sig, &sig_size))
    {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to sign manifest");
      delete manifest;
      goto sign_fail;
    }

    // Write new manifest
    FILE *fmanifest = fopen(manifest_path.c_str(), "w");
    if (!fmanifest) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to open manifest (errno: %d)",
               errno);
      delete manifest;
      goto sign_fail;
    }
    if ((fwrite(signed_manifest.data(), 1, signed_manifest.length(), fmanifest)
         != signed_manifest.length()) ||
        (fwrite(sig, 1, sig_size, fmanifest) != sig_size))
    {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to write manifest (errno: %d)",
               errno);
      fclose(fmanifest);
      delete manifest;
      goto sign_fail;
    }
    free(sig);
    fclose(fmanifest);

    // Upload manifest
    spooler->Upload(manifest_path, ".cvmfspublished");
    spooler->WaitForUpload();

    unlink(manifest_path.c_str());
    if (spooler->GetNumberOfErrors()) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to commit manifest (errors: %d)",
               spooler->GetNumberOfErrors());
      delete manifest;
      goto sign_fail;
    }

    delete manifest;
  }

  delete spooler;
  free(cert_buf);
  signature_manager.Fini();
  return 0;

 sign_fail:
  delete spooler;
  signature_manager.Fini();
  if (cert_buf) free(cert_buf);
  return 1;
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
