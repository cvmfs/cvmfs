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
#include "manifest.h"

using namespace std;

static void Usage() {
  LogCvmfs(kLogCvmfs, kLogStdout,
    "This tool signs a CernVM-FS file catalog.\n"
    "Version %s\n"
    "Usage:\n"
    "  cvmfs_sign [-c <x509 certificate>] [-k <private key>] [-s <password>]\n"
    "             [-n <repository name>] -m <manifest> -t <temp storage>\n"
    "             -p <paths_out (pipe)> -d <digests_in (pipe)>\n"
    "             [-l(ocal spooler) <local upstream path>]\n",
    VERSION);
}

int main(int argc, char **argv) {
  if (argc < 2) {
    Usage();
    return 1;
  }
  
  umask(022);

  string dir_temp = "";
  string certificate = "";
  string priv_key = "";
  string pwd = "";
  string repo_name = "";
  string manifest_path = "";
  string temp_dir = "";
  string paths_out = "";
  string digests_in = "";
  string local_upstream = "";
  bool local_spooler = false;
  upload::Spooler *spooler = NULL;  

  char c;
  while ((c = getopt(argc, argv, "c:k:s:n:m:t:p:d:l:h")) != -1) {
    switch (c) {
      case 'c':
        certificate = optarg;
        break;
      case 'k':
        priv_key = optarg;
        break;
      case 's':
        pwd = optarg;
        break;
      case 'n':
        repo_name = optarg;
        break;
      case 'm':
        manifest_path = optarg;
        break;
      case 't':
        temp_dir = MakeCanonicalPath(optarg);
        break;
      case 'p':
        paths_out = optarg;
        break;
      case 'd':
        digests_in = optarg;
        break;
      case 'l':
        local_spooler = true;
        local_upstream = MakeCanonicalPath(optarg);
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
  if ((manifest_path == "") || (paths_out == "") || (digests_in == "") ||
      (temp_dir == "")) 
  {
    Usage();
    return 1;
  }
  
  if (!DirectoryExists(temp_dir)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "%s does not exist", temp_dir.c_str());
    return 1;
  }
  
  // Optionally start the local "mini spooler"
  if (local_spooler) {
    int pid = fork();
    assert(pid >= 0);
    if (pid == 0) {
      return upload::MainLocalSpooler(paths_out, digests_in, local_upstream);
    }
  }
  
  // Connect to the spooler
  spooler = new upload::Spooler(paths_out, digests_in);
  bool retval = spooler->Connect();
  if (!retval) {
    PrintError("Failed to connect to spooler");
    return 1;
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

  
  LogCvmfs(kLogCvmfs, kLogStdout, "Signing %s", manifest_path.c_str());
  {
    // Load Manifest
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
    const string cert_path_tmp = temp_dir + "/cvmfspublisher.tmp";
    if (!CopyMem2Path((unsigned char *)compr_buf, compr_size, cert_path_tmp)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to save certificate");
      goto sign_fail;
    }
    free(compr_buf);

    const string cert_hash_path = "data" + certificate_hash.MakePath(1, 2) 
                                  + "X";
    spooler->SpoolCopy(cert_path_tmp, cert_hash_path);

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
      unlink(cert_path_tmp.c_str());
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
      unlink(cert_path_tmp.c_str());
      goto sign_fail;
    }
    free(sig);
    fclose(fmanifest);
    
    // Upload manifest
    spooler->SpoolCopy(manifest_path, ".cvmfspublished");
    
    spooler->EndOfTransaction();
    while (!spooler->IsIdle()) {
      sleep(1);
    }
    unlink(cert_path_tmp.c_str());
    unlink(manifest_path.c_str());
    if (spooler->num_errors()) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to commit manifest");
      goto sign_fail;
    }
  }

  delete spooler;
  signature::Fini();
  return 0;

 sign_fail:
  delete spooler;
  signature::Fini();
  return 1;
}

