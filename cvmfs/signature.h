/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SIGNATURE_H_
#define CVMFS_SIGNATURE_H_

#include <string>
#include <vector>
#include <cstdio>

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/err.h>
#include <openssl/bio.h>
#include <openssl/rsa.h>
#include <openssl/engine.h>

#include "hash.h"

namespace signature {

class SignatureManager {
 public:
  SignatureManager();

  void Init();
  void Fini();
  std::string GetCryptoError();

  bool LoadPrivateKeyPath(const std::string &file_pem,
                          const std::string &password);
  void UnloadPrivateKey();

  bool LoadCertificatePath(const std::string &file_pem);
  bool LoadCertificateMem(const unsigned char *buffer,
                          const unsigned buffer_size);
  bool WriteCertificateMem(unsigned char **buffer, unsigned *buffer_size);
  bool KeysMatch();
  bool VerifyCaChain();
  std::string Whois();
  std::string FingerprintCertificate(const shash::Algorithms hash_algorithm);

  bool LoadPublicRsaKeys(const std::string &path_list);
  bool LoadBlacklist(const std::string &path_blacklist);
  std::vector<std::string> GetBlacklistedCertificates();

  bool LoadTrustedCaCrl(const std::string &path_list);

  bool Sign(const unsigned char *buffer, const unsigned buffer_size,
            unsigned char **signature, unsigned *signature_size);
  bool Verify(const unsigned char *buffer, const unsigned buffer_size,
              const unsigned char *signature, unsigned signature_size);
  bool VerifyRsa(const unsigned char *buffer, const unsigned buffer_size,
                 const unsigned char *signature, unsigned signature_size);
  bool VerifyLetter(const unsigned char *buffer, const unsigned buffer_size,
                    const bool by_rsa);
  bool VerifyPkcs7(const unsigned char *buffer, const unsigned buffer_size,
                   unsigned char **content, unsigned *content_size,
                   std::vector<std::string> *alt_uris);
 private:
  void InitX509Store();
  void CutLetter(const unsigned char *buffer, const unsigned buffer_size,
                 unsigned *letter_length, unsigned *pos_after_mark);

  EVP_PKEY *private_key_;
  X509 *certificate_;
  std::vector<RSA *> public_keys_;  /**< Contains cvmfs public master keys */
  std::vector<std::string> blacklisted_certificates_;
  X509_STORE *x509_store_;
  X509_LOOKUP *x509_lookup_;
};  // class SignatureManager

}  // namespace signature

#endif  // CVMFS_SIGNATURE_H_
