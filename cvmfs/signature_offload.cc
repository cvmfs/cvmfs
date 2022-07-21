/**
 * This file is part of the CernVM File System.
 *
 * A standalone utility that executes SignatureManager::Sign().
 * This is a transitional workaround for EL9 / OpenSSL 3; it is necessary
 * because setuid binaries (and binaries with elevated file capailities)
 * cannot use SignatureManager::Sign themselves even with the environemnt
 * variable OPENSSL_ENABLE_SHA1_SIGNATURES defined.
 * It buys us time until a proper fix is found.
 *
 * This utility reads the private key and the buffer to sign from stdin and
 * writes the signature to stdout. In case of errors, it writes an empty
 * signature.
 */

#include <string>

#include "signature.h"
#include "util/posix.h"

static std::string ReadString() {
  unsigned size;
  ReadPipe(0, &size, sizeof(size));
  std::string result;
  result.resize(size);
  ReadPipe(0, const_cast<char *>(result.data()), size);
  return result;
}

int main() {
  signature::SignatureManager::ESignMethod method;
  ReadPipe(0, &method, sizeof(method));
  std::string key = ReadString();
  std::string buffer = ReadString();

  signature::SignatureManager smgr;
  smgr.Init();

  unsigned char *signature = NULL;
  unsigned signature_size = 0;

  bool rv_key = false;
  if (method == signature::SignatureManager::kSignWhitelist) {
    rv_key = smgr.LoadPrivateMasterKeyMem(key);
  } else if (method == signature::SignatureManager::kSignManifest) {
    rv_key = smgr.LoadPrivateKeyMem(key);
  }
  if (!rv_key) {
    WritePipe(1, &signature_size, sizeof(signature_size));
    smgr.Fini();
    return 1;
  }

  bool rv_sign = false;
  if (method == signature::SignatureManager::kSignWhitelist) {
    rv_sign = smgr.SignRsa(reinterpret_cast<unsigned char *>(buffer.data()),
                           buffer.size(), &signature, &signature_size);
  } else if (method == signature::SignatureManager::kSignManifest) {
    rv_sign = smgr.Sign(reinterpret_cast<unsigned char *>(buffer.data()),
                        buffer.size(), &signature, &signature_size);
  }
  if (!rv_sign) {
    WritePipe(1, &signature_size, sizeof(signature_size));
    smgr.Fini();
    return 1;
  }

  WritePipe(1, &signature_size, sizeof(signature_size));
  WritePipe(1, signature, signature_size);
  free(signature);

  smgr.Fini();
  return 0;
}
