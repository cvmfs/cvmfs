/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_LETTER_H_
#define CVMFS_LETTER_H_

#include <string>

#include "hash.h"

namespace signature {
class SignatureManager;
}

namespace letter {

enum Failures {
  kFailOk = 0,
  kFailBadBase64,
  kFailMalformed,
  kFailExpired,
  kFailBadSignature,
  kFailBadCertificate,
  kFailNameMismatch,

  kFailNumEntries
};

inline const char *Code2Ascii(const Failures error) {
  const char *texts[kFailNumEntries + 1];
  texts[0] = "OK";
  texts[1] = "invalid Base64 input";
  texts[2] = "letter malformed";
  texts[3] = "letter expired";
  texts[4] = "signature verification failed";
  texts[5] = "certificate is not whitelisted";
  texts[6] = "repository name mismatch";
  texts[7] = "no text";
  return texts[error];
}

class Letter {
 public:
  Letter(const std::string &fqrn,
         const std::string &text,
         signature::SignatureManager *signature_manager);
  std::string Sign(const shash::Algorithms hash_algorithm);
  Failures Verify(uint64_t max_age, std::string *msg, std::string *cert);

 private:
  std::string fqrn_;
  std::string text_;
  signature::SignatureManager *signature_manager_;
};  // class Letter

}  // namespace letter

#endif  // CVMFS_LETTER_H_
