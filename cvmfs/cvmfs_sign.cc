/**
 * This file is part of the CernVM File System
 *
 * This tool signs a CernVM-FS file catalog and its nested catalogs
 * with an X.509 certificate.
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

using namespace std;

static void Usage() {
  LogCvmfs(kLogCvmfs, kLogStdout,
    "This tool signs a CernVM-FS file catalog.\n"
    "Version %s\n"
    "Usage:\n"
    "  cvmfs_sign [-c <x509 certificate>] [-k <private key>] [-p <password>]\n"
    "             [-n <repository name>] <catalog>",
    VERSION);
}

int main(int argc, char **argv) {
  if (argc < 2) {
    Usage();
    return 1;
  }

  string dir_catalogs = "";
  string certificate = "";
  string priv_key = "";
  string pwd = "";
  string repo_name = "";

  char c;
  while ((c = getopt(argc, argv, "c:k:p:n:h")) != -1) {
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
      case 'h':
        Usage();
        return 0;
      case '?':
      default:
        abort();
    }
  }
  if (optind >= argc) {
    Usage();
    return 1;
  }

  dir_catalogs = MakeCanonicalPath(GetParentPath(string(argv[optind])));
  const string clg_path = dir_catalogs + "/.cvmfscatalog.working";
  const string snapshot_path = dir_catalogs + "/.cvmfscatalog";
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

  LogCvmfs(kLogCvmfs, kLogStdout, "Signing %s", snapshot_path.c_str());
  {
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
    const string cert_path_tmp = dir_catalogs + "/data/txn/cvmfspublisher.tmp";
    int fd_cert;
    FILE *fcert;
    if ( ((fd_cert = open(cert_path_tmp.c_str(), O_CREAT | O_TRUNC | O_RDWR,
                          kDefaultFileMode)) < 0)
        ||
        !(fcert = fdopen(fd_cert, "w")) )
    {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to save certificate");
      goto sign_fail;
    }
    if (static_cast<int64_t>(fwrite(compr_buf, 1, compr_size, fcert)) <
        compr_size)
    {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to save certificate");
      goto sign_fail;
    }
    fclose(fcert);
    free(compr_buf);

    const string cert_hash_path = certificate_hash.MakePath(1, 2) + "X";
    const string cert_path = dir_catalogs + "/data/" + cert_hash_path;
    if (rename(cert_path_tmp.c_str(), cert_path.c_str()) != 0) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to store certificate in %s",
               cert_path.c_str());
      goto sign_fail;
    }

    // Write extended checksum
    map<char, string> content;
    if (!ParseKeyvalPath(dir_catalogs + "/.cvmfspublished", &content) ||
        (content.find('C') == content.end()))
    {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to read extended cvmfspublished");
    } else {
      content['X'] = certificate_hash.ToString();
      content['T'] = StringifyInt(time(NULL));
      if (repo_name != "")
        content['N'] = repo_name;

      string rest;
      rest = 'C' + content['C'] + "\n";
      for (map<char, string>::const_iterator itr = content.begin(),
           itrEnd = content.end(); itr != itrEnd; ++itr)
      {
        if (itr->first == 'C')
          continue;
        rest += itr->first + itr->second + "\n";
      }
      hash::Any published_hash(hash::kSha1);
      hash::HashMem(reinterpret_cast<const unsigned char *>(rest.data()),
                    rest.length(), &published_hash);
      rest += "--\n" + published_hash.ToString() + "\n";

      FILE *fext = fopen((dir_catalogs + "/.cvmfspublished").c_str(), "w");
      if (!fext) {
        LogCvmfs(kLogCvmfs, kLogStderr, "Failed to write .cvmfspublished");
        goto sign_fail;
      }
      if (fwrite(rest.data(), 1, rest.length(), fext) != rest.length()) {
        LogCvmfs(kLogCvmfs, kLogStderr, "Failed to write .cvmfspublished");
        fclose(fext);
        goto sign_fail;
      }

      // Sign checksum and write signature
      unsigned char *sig;
      unsigned sig_size;
      if (!signature::Sign(reinterpret_cast<const unsigned char *>(
                             published_hash.ToString().data()),
                           2*published_hash.GetDigestSize(), &sig, &sig_size))
      {
        LogCvmfs(kLogCvmfs, kLogStderr, "Failed to sign extended checksum");
        fclose(fext);
        goto sign_fail;
      }
      if (fwrite(sig, 1, sig_size, fext) != sig_size) {
        LogCvmfs(kLogCvmfs, kLogStderr, "Failed to write .cvmfspublished");
        free(sig);
        fclose(fext);
        goto sign_fail;
      }
      free(sig);

      fclose(fext);
    }
  }

  signature::Fini();
  return 0;

 sign_fail:
  signature::Fini();
  return 1;
}

