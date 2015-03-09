/**
 * This file is part of the CernVM File System
 *
 * This tool signs a CernVM-FS manifest with an X.509 certificate.
 */

#include "cvmfs_config.h"
#include "swissknife_letter.h"

#include <inttypes.h>
#include <termios.h>

#include <cassert>

#include "download.h"
#include "hash.h"
#include "letter.h"
#include "signature.h"
#include "util.h"
#include "whitelist.h"

using namespace std;  // NOLINT


static void ReadStdinBytes(unsigned char *buf, const uint16_t num_bytes) {
  int read_chunk;
  unsigned read_all = 0;

  do {
    if ((read_chunk = read(0, buf+read_all, num_bytes-read_all)) <= 0)
      break;
    read_all += read_chunk;
  } while (read_all < num_bytes);

  if (read_chunk == 0) exit(0);
  assert(read_all == num_bytes);
}


static void WriteStdoutBytes(const unsigned char *buf,
                             const uint16_t num_bytes)
{
  int wrote_chunk;
  unsigned wrote_all = 0;

  do {
    if ((wrote_chunk = write(1, buf+wrote_all, num_bytes-wrote_all)) <= 0)
      break;
    wrote_all += wrote_chunk;
  } while (wrote_all < num_bytes);

  assert(wrote_all == num_bytes);
}


static uint16_t ReadErlang(unsigned char *buf) {
  int len;

  ReadStdinBytes(buf, 2);
  len = (buf[0] << 8) | buf[1];
  if (len > 0)
    ReadStdinBytes(buf, len);
  return len;
}


static void WriteErlang(const unsigned char *buf, int len) {
  unsigned char li;

  li = (len >> 8) & 0xff;
  WriteStdoutBytes(&li, 1);
  li = len & 0xff;
  WriteStdoutBytes(&li, 1);

  WriteStdoutBytes(buf, len);
}


int swissknife::CommandLetter::Main(const swissknife::ArgumentList &args) {
  bool verify = false;
  if (args.find('v') != args.end()) verify = true;
  if ((args.find('s') != args.end()) && verify) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "invalid option combination (sign + verify)");
    return 1;
  }

  bool erlang = false;
  string repository_url;
  string certificate_path;
  string certificate_password;
  shash::Algorithms hash_algorithm = shash::kSha1;
  uint64_t max_age;
  if (verify) {
    repository_url = *args.find('r')->second;
    max_age = String2Uint64(*args.find('m')->second);
    if (args.find('e') != args.end()) erlang = true;
  } else {
    certificate_path = *args.find('c')->second;
    if (args.find('p') != args.end())
      certificate_password = *args.find('p')->second;
    if (args.find('a') != args.end()) {
      hash_algorithm = shash::ParseHashAlgorithm(*args.find('a')->second);
      if (hash_algorithm == shash::kAny) {
        LogCvmfs(kLogCvmfs, kLogStderr, "unknown hash algorithm");
        return 1;
      }
    }
  }
  string fqrn;
  string text;
  string key_path;
  string cacrl_path;
  fqrn = *args.find('f')->second;
  key_path = *args.find('k')->second;
  if (args.find('t') != args.end()) text = *args.find('t')->second;
  if (args.find('z') != args.end()) cacrl_path = *args.find('z')->second;

  bool retval_b;
  whitelist::Failures retval_wl;
  letter::Failures retval_ltr;
  signature::SignatureManager signature_manager;
  signature_manager.Init();

  if (verify) {
    if (cacrl_path != "") {
      retval_b = signature_manager.LoadTrustedCaCrl(cacrl_path);
      if (!retval_b) {
        LogCvmfs(kLogCvmfs, kLogStderr, "failed to load CA/CRLs");
        return 2;
      }
    }
    retval_b = signature_manager.LoadPublicRsaKeys(key_path);
    if (!retval_b && (cacrl_path == "")) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to load public keys");
      return 2;
    }

    download::DownloadManager download_manager;
    download_manager.Init(2, false);
    whitelist::Whitelist whitelist(fqrn, &download_manager, &signature_manager);
    retval_wl = whitelist.Load(repository_url);
    if (retval_wl != whitelist::kFailOk) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to load whitelist (%d): %s",
               retval_wl, whitelist::Code2Ascii(retval_wl));
      return 2;
    }

    if (erlang) {
      const char *ready = "ready";
      WriteErlang(reinterpret_cast<const unsigned char *>(ready), 5);
    }

    char exit_code = 0;
    do {
      if (erlang) {
        unsigned char buf[65000];
        int length = ReadErlang(buf);
        text = string(reinterpret_cast<char *>(buf), length);
      } else {
        if (text == "") {
          char c;
          int num_read;
          while ((num_read = read(0, &c, 1)) == 1) {
            if (c == '\n')
              break;
            text.push_back(c);
          }
          if (num_read != 1) return exit_code;
        }
      }

      if ((time(NULL) + 3600*24*3) > whitelist.expires()) {
        LogCvmfs(kLogCvmfs, kLogStderr, "reloading whitelist");
        whitelist::Whitelist refresh(fqrn, &download_manager,
                                     &signature_manager);
        retval_wl = refresh.Load(repository_url);
        if (retval_wl == whitelist::kFailOk)
          whitelist = refresh;
      }

      string message;
      string cert;
      letter::Letter letter(fqrn, text, &signature_manager);
      retval_ltr = letter.Verify(max_age, &message, &cert);
      if (retval_ltr != letter::kFailOk) {
        exit_code = 3;
        LogCvmfs(kLogCvmfs, kLogStderr, "%s", letter::Code2Ascii(retval_ltr));
      } else {
        if (whitelist.IsExpired()) {
          exit_code = 4;
          LogCvmfs(kLogCvmfs, kLogStderr, "whitelist expired");
        } else {
          retval_wl = whitelist.VerifyLoadedCertificate();
          if (retval_wl == whitelist::kFailOk) {
            exit_code = 0;
          } else {
            exit_code = 5;
            LogCvmfs(kLogCvmfs, kLogStderr, "%s",
                     whitelist::Code2Ascii(retval_wl));
          }
        }
      }

      if (erlang) {
        if ((exit_code == 0) && (message.length() > 60000))
          exit_code = 6;
        WriteErlang(reinterpret_cast<unsigned char *>(&exit_code), 1);
        if (exit_code == 0)
          WriteErlang(reinterpret_cast<const unsigned char *>(message.data()),
                      message.length());
      } else {
        if (exit_code == 0)
          LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "%s",
                   message.c_str());
      }
      text = "";
    } while (erlang);
    download_manager.Fini();
    signature_manager.Fini();
    return exit_code;
  }

  // Load certificate
  if (!signature_manager.LoadCertificatePath(certificate_path)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load certificate");
    return 2;
  }

  // Load private key
  if (!signature_manager.LoadPrivateKeyPath(key_path, certificate_password)) {
    int retry = 0;
    bool success;
    do {
      struct termios defrsett, newrsett;
      tcgetattr(fileno(stdin), &defrsett);
      newrsett = defrsett;
      newrsett.c_lflag &= ~ECHO;
      if (tcsetattr(fileno(stdin), TCSAFLUSH, &newrsett) != 0) {
        LogCvmfs(kLogCvmfs, kLogStderr, "terminal failure");
        return 2;
      }

      LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
               "Enter password for private key: ");
      certificate_password = "";
      GetLineFd(0, &certificate_password);
      tcsetattr(fileno(stdin), TCSANOW, &defrsett);
      LogCvmfs(kLogCvmfs, kLogStdout, "");

      success =
        signature_manager.LoadPrivateKeyPath(key_path, certificate_password);
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
    return 2;
  }
  if (text == "") {
    char c;
    while (read(0, &c, 1) == 1) {
      if (c == '\n')
        break;
      text.push_back(c);
    }
  }

  letter::Letter text_letter(fqrn, text, &signature_manager);
  LogCvmfs(kLogCvmfs, kLogStdout, "%s",
           text_letter.Sign(hash_algorithm).c_str());

  signature_manager.Fini();
  return 0;
}
