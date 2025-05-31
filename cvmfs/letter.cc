/**
 * This file is part of the CernVM File System.
 */


#include "letter.h"

#include <inttypes.h>

#include <cassert>
#include <map>

#include "crypto/signature.h"
#include "util/string.h"

using namespace std;  // NOLINT

namespace letter {

Letter::Letter(const string &fqrn,
               const string &text,
               signature::SignatureManager *signature_manager)
    : fqrn_(fqrn), text_(text), signature_manager_(signature_manager) { }


string Letter::Sign(const shash::Algorithms hash_algorithm) {
  unsigned char *cert_buf = NULL;
  unsigned cert_buf_size;
  bool retval = signature_manager_->WriteCertificateMem(&cert_buf,
                                                        &cert_buf_size);
  assert(retval);
  const string cert_base64 =
      Base64(string(reinterpret_cast<char *>(cert_buf), cert_buf_size));
  free(cert_buf);

  string output = text_;
  output += string("\n##\n") + "V1" + "\n" + "N" + fqrn_ + "\n" + "T"
            + StringifyInt(time(NULL)) + "\n" + "X" + cert_base64 + "\n";
  shash::Any output_hash(hash_algorithm);
  shash::HashMem(reinterpret_cast<const unsigned char *>(output.data()),
                 output.length(), &output_hash);
  output += "--\n" + output_hash.ToString() + "\n";

  unsigned char *sig;
  unsigned sig_size;
  retval = signature_manager_->Sign(
      reinterpret_cast<const unsigned char *>(output_hash.ToString().data()),
      output_hash.GetHexSize(), &sig, &sig_size);
  assert(retval);
  output.append(reinterpret_cast<char *>(sig), sig_size);
  free(sig);

  return Base64(output);
}


Failures Letter::Verify(uint64_t max_age, string *msg, string *cert) {
  msg->clear();
  cert->clear();
  bool retval;

  string dec;
  retval = Debase64(text_, &dec);

  if (!retval)
    return kFailBadBase64;

  // Cut envelope and signature
  unsigned env_pos = 0;
  unsigned sig_pos = 0;
  unsigned msg_len = 0;
  unsigned env_len = 0;
  const unsigned char *data_ptr = reinterpret_cast<const unsigned char *>(
      dec.data());
  signature::SignatureManager::CutLetter(data_ptr, dec.length(), '#', &msg_len,
                                         &env_pos);
  if (env_pos >= dec.length())
    return kFailMalformed;
  // Sign adds a newline
  if (msg_len == 0)
    return kFailMalformed;
  *msg = dec.substr(0, msg_len - 1);
  signature::SignatureManager::CutLetter(
      data_ptr + env_pos, dec.length() - env_pos, '-', &env_len, &sig_pos);
  if (sig_pos >= dec.length())
    return kFailMalformed;

  map<char, string> env;
  ParseKeyvalMem(data_ptr + env_pos, env_len, &env);
  map<char, string>::const_iterator iter;
  if ((iter = env.find('T')) == env.end())
    return kFailMalformed;
  const uint64_t timestamp = String2Uint64(iter->second);
  if (max_age > 0) {
    if (timestamp + max_age < static_cast<uint64_t>(time(NULL)))
      return kFailExpired;
  }
  if ((iter = env.find('N')) == env.end())
    return kFailMalformed;
  if (iter->second != fqrn_)
    return kFailNameMismatch;
  if ((iter = env.find('X')) == env.end())
    return kFailMalformed;
  const string cert_b64 = iter->second;
  retval = Debase64(cert_b64, cert);
  if (!retval)
    return kFailMalformed;
  retval = signature_manager_->LoadCertificateMem(
      reinterpret_cast<const unsigned char *>(cert->data()), cert->length());
  if (!retval)
    return kFailBadCertificate;

  retval = signature_manager_->VerifyLetter(data_ptr, dec.length(), false);
  if (!retval)
    return kFailBadSignature;

  return kFailOk;
}

}  // namespace letter
