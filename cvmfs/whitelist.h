/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_WHITELIST_H_
#define CVMFS_WHITELIST_H_

#include <gtest/gtest_prod.h>
#include <inttypes.h>

#include <ctime>
#include <string>
#include <vector>

#include "crypto/hash.h"

namespace download {
class DownloadManager;
}

namespace signature {
class SignatureManager;
}


namespace whitelist {

enum Failures {
  kFailOk = 0,
  kFailLoad,
  kFailEmpty,
  kFailMalformed,
  kFailNameMismatch,
  kFailExpired,
  kFailBadSignature,
  kFailLoadPkcs7,
  kFailEmptyPkcs7,
  kFailMalformedPkcs7,
  kFailBadSignaturePkcs7,
  kFailBadPkcs7,
  kFailBadCaChain,
  kFailNotListed,
  kFailBlacklisted,

  kFailNumEntries
};


inline const char *Code2Ascii(const Failures error) {
  const char *texts[kFailNumEntries + 1];
  texts[0] = "OK";
  texts[1] = "failed to download whitelist";
  texts[2] = "empty whitelist";
  texts[3] = "malformed whitelist";
  texts[4] = "repository name mismatch on whitelist";
  texts[5] = "expired whitelist";
  texts[6] = "invalid whitelist signature";
  texts[7] = "failed to download whitelist (pkcs7)";
  texts[8] = "empty whitelist (pkcs7)";
  texts[9] = "malformed whitelist (pkcs7)";
  texts[10] = "invalid whitelist signer (pkcs7)";
  texts[11] = "invalid whitelist (pkcs7)";
  texts[12] = "failed to verify CA chain";
  texts[13] = "certificate not on whitelist";
  texts[14] = "certificate blacklisted";
  texts[15] = "no text";
  return texts[error];
}


class Whitelist {
  FRIEND_TEST(T_Whitelist, ParseWhitelist);

 public:
  enum Status {
    kStNone,
    kStAvailable,
  };

  static std::string CreateString(
      const std::string &fqrn,
      int validity_days,
      shash::Algorithms hash_algorithm,
      signature::SignatureManager *signature_manager);

  Whitelist(const std::string &fqrn,
            download::DownloadManager *download_manager,
            signature::SignatureManager *signature_manager);
  ~Whitelist();
  explicit Whitelist(const Whitelist &other);
  Whitelist &operator=(const Whitelist &other);
  Failures LoadUrl(const std::string &base_url);
  Failures LoadMem(const std::string &whitelist);

  void CopyBuffers(unsigned *plain_size, unsigned char **plain_buf,
                   unsigned *pkcs7_size, unsigned char **pkcs7_buf) const;
  time_t expires() const;
  bool IsExpired() const;
  Failures VerifyLoadedCertificate() const;

  std::string ExportString() const;

  Status status() const { return status_; }

 private:
  Whitelist();

  static const int kFlagVerifyRsa;
  static const int kFlagVerifyPkcs7;
  static const int kFlagVerifyCaChain;

  bool IsBefore(time_t now, const struct tm &t_whitelist);
  Failures VerifyWhitelist();
  Failures ParseWhitelist(const unsigned char *whitelist,
                          const unsigned whitelist_size);
  void Reset();

  std::string fqrn_;
  download::DownloadManager *download_manager_;
  signature::SignatureManager *signature_manager_;

  Status status_;
  std::vector<shash::Any> fingerprints_;
  time_t expires_;
  int verification_flags_;
  unsigned char *plain_buf_;
  unsigned plain_size_;
  unsigned char *pkcs7_buf_;
  unsigned pkcs7_size_;
};

}  // namespace whitelist

#endif  // CVMFS_WHITELIST_H_
