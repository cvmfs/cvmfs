/**
 * This file is part of the CernVM File System
 *
 * This tool signs a CernVM-FS manifest with an X.509 certificate.
 */

#include "cvmfs_config.h"
#include "swissknife_letter.h"

#include <termios.h>

#include "signature.h"
#include "hash.h"
#include "util.h"
#include "letter.h"

using namespace std;  // NOLINT

int swissknife::CommandLetter::Main(const swissknife::ArgumentList &args) {
  string text = "";
  if (args.find('t') != args.end()) text = *args.find('t')->second;
  string certificate = "";
  if (args.find('c') != args.end()) certificate = *args.find('c')->second;
  string priv_key = "";
  if (args.find('k') != args.end()) priv_key = *args.find('k')->second;
  string pwd = "";
  if (args.find('s') != args.end()) pwd = *args.find('s')->second;
  shash::Algorithms hash_algorithm = shash::kSha1;
  if (args.find('a') != args.end()) {
    hash_algorithm = shash::ParseHashAlgorithm(*args.find('a')->second);
    if (hash_algorithm == shash::kAny) {
      LogCvmfs(kLogCvmfs, kLogStderr, "unknown hash algorithm");
      return 1;
    }
  }
  bool verify = false;
  if (args.find('v') != args.end())
  verify = true;

  signature::SignatureManager signature_manager;
  signature_manager.Init();

  // Load certificate
  if (certificate == "") {
    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
             "Enter file name of X509 certificate []: ");
    GetLineFd(0, &certificate);
  }
  if (!signature_manager.LoadCertificatePath(certificate)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load certificate");
    return 2;
  }

  // Load private key
  if (priv_key == "") {
    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
             "Enter file name of private key file to your certificate []: ");
    GetLineFd(0, &priv_key);
  }
  if (!signature_manager.LoadPrivateKeyPath(priv_key, pwd)) {
    int retry = 0;
    bool success;
    do {
      struct termios defrsett, newrsett;
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
      GetLineFd(0, &pwd);
      tcsetattr(fileno(stdin), TCSANOW, &defrsett);
      LogCvmfs(kLogCvmfs, kLogStdout, "");

      success = signature_manager.LoadPrivateKeyPath(priv_key, pwd);
      if (!success) {
        LogCvmfs(kLogCvmfs, kLogStderr, "failed to load private key (%s)",
                 signature_manager.GetCryptoError().c_str());
      }
      retry++;
    } while (!success && (retry < 3));
    if (!success)
      return 2;
  }
  if (!signature_manager.KeysMatch()) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "the private key doesn't seem to match your certificate (%s)",
             signature_manager.GetCryptoError().c_str());
    signature_manager.UnloadPrivateKey();
    return 2;
  }
  if (text == "") {
    char c;
    while (read(0, &c, 1) == 1) {
      if (c == '\n')
        break;
      text.push_back(c);
    }
    // TODO: read from stdin
  }

  letter::Letter text_letter(text, &signature_manager);

  if (verify) {
    letter::Failures retval;
    string msg;
    string cert;
    retval = text_letter.Verify(5, &msg, &cert);
    LogCvmfs(kLogCvmfs, kLogStdout, "retval %d (%s), msg\n%s\ncert\n%s",
             retval, letter::Code2Ascii(retval), msg.c_str(), cert.c_str());
    return 0;
  }

  LogCvmfs(kLogCvmfs, kLogStdout, "%s",
           text_letter.Sign(hash_algorithm).c_str());

  signature_manager.Fini();
  return 0;
}
