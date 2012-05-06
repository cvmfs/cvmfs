/**
 * This file is part of the CernVM File System
 *
 * This tool signs a CernVM-FS manifest with an X.509 certificate.
 */

#include "cvmfs_config.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <termios.h>
#include <dirent.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>

#include <iostream>
#include <sstream>
#include <string>
#include <set>
#include <vector>

#include "smalloc.h"
#include "signature.h"
#include "hash.h"
#include "util.h"
#include "compression.h"
#include "logging.h"
#include "upload.h"
#include "download.h"
#include "manifest.h"

using namespace std;

static void Usage() {
  LogCvmfs(kLogCvmfs, kLogStdout,
    "This tool signs a CernVM-FS file catalog.\n"
    "Version %s\n"
    "Usage:\n"
    "  cvmfs_sign [-c <x509 certificate>] [-k <private key>] [-p <password>]\n"
    "             [-n <repository name>] -t <temp directory>\n"
    "             -r <upstream storage> -u <base url>\n",
    VERSION);
}

int main(int argc, char **argv) {
  if (argc < 2) {
    Usage();
    return 1;
  }

  string dir_temp = "";
  string certificate = "";
  string priv_key = "";
  string pwd = "";
  string repo_name = "";
  string base_url = "";
  upload::Forklift *forklift;

  char c;
  while ((c = getopt(argc, argv, "c:k:p:n:t:r:u:h")) != -1) {
    switch (c) {
      case 'c':
        certificate = optarg;
        break;
      case 'k':
        priv_key = optarg;
        break;
      case 'p':
        pwd = optarg;
        break;
      case 'n':
        repo_name = optarg;
        break;
      case 't':
        dir_temp = optarg;
        break;
      case 'r':
        forklift = upload::CreateForklift(optarg);
        break;
      case 'u':
        base_url = optarg;
        break;
      case 'h':
        Usage();
        return 0;
      case '?':
      default:
        abort();
    }
  }
  
  // Sanity checks
  if ((base_url == "") || (dir_temp == "") || !forklift) {
    Usage();
    return 1;
  }
  if (!forklift->Connect()) {
    LogCvmfs(kLogCvmfs, kLogStderr, 
             "failed to connect to upstream storage (%s)",
             forklift->GetLastError().c_str());
    return 2;
  }
  if (!DirectoryExists(dir_temp)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "temporary directory %s does not exist", 
             dir_temp.c_str());
    return 2;
  }

  signature::Init();

  // Load certificate
  unsigned char *cert_buf;
  unsigned cert_buf_size;
  if (certificate == "") {
    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
             "Enter file name of X509 certificate []: ");
    getline(cin, certificate);
  }
  if (!signature::LoadCertificatePath(certificate) ||
      !signature::WriteCertificateMem(&cert_buf, &cert_buf_size))
  {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load certificate");
    return 2;
  }

  // Load private key
  if (priv_key == "") {
    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
             "Enter file name of private key file to your certificate []: ");
    getline(cin, priv_key);
  }
  if (!signature::LoadPrivateKeyPath(priv_key, pwd)) {
    int retry = 0;
    bool success;
    do {
      struct termios defrsett, newrsett;
      char c;
      tcgetattr(fileno(stdin), &defrsett);
      newrsett = defrsett;
      newrsett.c_lflag &= ~ECHO;
      if(tcsetattr(fileno(stdin), TCSAFLUSH, &newrsett) != 0) {
        LogCvmfs(kLogCvmfs, kLogStderr, "terminal failure");
        return 2;
      }

      LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
               "Enter password for private key: ");
      pwd = "";
      while (cin.get(c) && (c != '\n'))
        pwd += c;
      tcsetattr(fileno(stdin), TCSANOW, &defrsett);
      LogCvmfs(kLogCvmfs, kLogStdout, "");

      success = signature::LoadPrivateKeyPath(priv_key, pwd);
      if (!success) {
        LogCvmfs(kLogCvmfs, kLogStderr, "failed to load private key (%s)",
                 signature::GetCryptoError().c_str());
      }
      retry++;
    } while (!success && (retry < 3));
    if (!success)
      return 2;
  }
  if (!signature::KeysMatch()) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "the private key doesn't seem to match your certificate (%s)",
             signature::GetCryptoError().c_str());
    signature::UnloadPrivateKey();
    return 2;
  }

  
  const string manifest_url = base_url + "/.cvmfspublished";
  const string manifest_path = dir_temp + "/manifest";
  download::Init(1);
  LogCvmfs(kLogCvmfs, kLogStdout, "Signing %s", manifest_url.c_str());
  {
    // Retrieve Manifest
    download::JobInfo download_manifest(&manifest_url, false, false, 
                                        &manifest_path, NULL);    
    download::Failures retval = download::Fetch(&download_manifest);
    if (retval != download::kFailOk) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to fetch manifest");
      goto sign_fail;
    }
    Manifest *manifest = Manifest::LoadFile(manifest_path);
    if (!manifest) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to parse manifest");
      goto sign_fail;
    }
    
    // Safe certificate
    void *compr_buf;
    int64_t compr_size;
    if (!zlib::CompressMem2Mem(cert_buf, cert_buf_size,
                               &compr_buf, &compr_size))
    {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to compress certificate");
      goto sign_fail;
    }
    hash::Any certificate_hash(hash::kSha1);
    hash::HashMem((unsigned char *)compr_buf, compr_size, &certificate_hash);
    const string cert_path_tmp = dir_temp + "/cvmfspublisher.tmp";
    if (!CopyMem2Path((unsigned char *)compr_buf, compr_size, cert_path_tmp)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to save certificate");
      goto sign_fail;
    }
    free(compr_buf);

    const string cert_hash_path = "/data" + certificate_hash.MakePath(1, 2) 
                                  + "X";
    if (!forklift->Move(cert_path_tmp, cert_hash_path)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to commit certificate (%s)",
               forklift->GetLastError().c_str());
      unlink(cert_path_tmp.c_str());
      goto sign_fail;
    }

    // Update manifest
    manifest->set_certificate(certificate_hash);
    manifest->set_repository_name(repo_name);
    manifest->set_publish_timestamp(time(NULL));
    
    string signed_manifest = manifest->ExportString();
    hash::Any published_hash(hash::kSha1);
    hash::HashMem(
      reinterpret_cast<const unsigned char *>(signed_manifest.data()),
      signed_manifest.length(), &published_hash);
    signed_manifest += "--\n" + published_hash.ToString() + "\n";

    // Sign manifest
    unsigned char *sig;
    unsigned sig_size;
    if (!signature::Sign(reinterpret_cast<const unsigned char *>(
                         published_hash.ToString().data()),
                         2*published_hash.GetDigestSize(), &sig, &sig_size))
    {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to sign manifest");
      goto sign_fail;
    }
    
    // Write new manifest
    FILE *fmanifest = fopen(manifest_path.c_str(), "w");
    if (!fmanifest) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to write manifest");
      goto sign_fail;
    }
    if ((fwrite(signed_manifest.data(), 1, signed_manifest.length(), fmanifest) 
         != signed_manifest.length()) ||
        (fwrite(sig, 1, sig_size, fmanifest) != sig_size)) 
    {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to write manifest");
      fclose(fmanifest);
      goto sign_fail;
    }
    free(sig);
    fclose(fmanifest);
    
    // Upload manifest
    if (!forklift->Move(manifest_path, "/.cvmfspublished")) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to commit manifest");
      unlink(manifest_path.c_str());
      goto sign_fail;
    }
  }

  delete forklift;
  signature::Fini();
  download::Fini();
  return 0;

 sign_fail:
  delete forklift;
  signature::Fini();
  download::Fini();
  unlink(manifest_path.c_str());
  return 1;
}

