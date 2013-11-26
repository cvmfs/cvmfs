/**
 * This file is part of the CernVM File System.
 *
 * This is a wrapper around OpenSSL's libcrypto.  It supports
 * signing of data with an X.509 certificate and verifiying
 * a signature against a certificate.  The certificates act only as key
 * store, there is no verification against the CA chain.
 *
 * It also supports verification of plain RSA signatures (for the whitelist).
 *
 * We work exclusively with PEM formatted files (= Base64-encoded DER files).
 */

#include "cvmfs_config.h"
#include "signature.h"

#include <openssl/pkcs7.h>

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cctype>
#include <cassert>

#include <string>
#include <vector>

#include "platform.h"
#include "logging.h"
#include "hash.h"
#include "util.h"
#include "smalloc.h"
#include "compression.h"

using namespace std;  // NOLINT

namespace signature {

const char *kDefaultPublicKey = "/etc/cvmfs/keys/cern.ch.pub";


static int CallbackCertVerify(int ok, X509_STORE_CTX *ctx) {
  if (ok) return ok;

  int error = X509_STORE_CTX_get_error(ctx);
  X509 *current_cert = X509_STORE_CTX_get_current_cert(ctx);
  string subject = "subject n/a";
  if (current_cert) {
    char *buffer = NULL;
    buffer = X509_NAME_oneline(X509_get_subject_name(current_cert), NULL, 0);
    if (buffer) {
      subject = string(buffer);
      free(buffer);
    }
  }
  LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
           "certificate verification error: %s, error %s (%d)",
           subject.c_str(), X509_verify_cert_error_string(error), error);
  return ok;
}


SignatureManager::SignatureManager() {
  private_key_ = NULL;
  certificate_ = NULL;
  x509_store_ = NULL;
  x509_lookup_ = NULL;
}


void SignatureManager::InitX509Store() {
  if (x509_store_) X509_STORE_free(x509_store_);
  x509_store_ = X509_STORE_new();
  assert(x509_store_ != NULL);

  int retval;
  X509_VERIFY_PARAM *param = X509_VERIFY_PARAM_new();
  assert(param != NULL);
  unsigned long verify_flags =
    X509_V_FLAG_CRL_CHECK |
    X509_V_FLAG_CRL_CHECK_ALL |
    X509_V_FLAG_POLICY_CHECK;
  retval = X509_VERIFY_PARAM_set_flags(param, verify_flags);
  assert(retval == 1);
  retval = X509_STORE_set1_param(x509_store_, param);
  assert(retval == 1);
  X509_VERIFY_PARAM_free(param);

  X509_LOOKUP *lookup =
    X509_STORE_add_lookup(x509_store_, X509_LOOKUP_hash_dir());
  assert(lookup != NULL);

  X509_STORE_set_verify_cb_func(x509_store_, CallbackCertVerify);
}


void SignatureManager::Init() {
  OpenSSL_add_all_algorithms();
  InitX509Store();
}


void SignatureManager::Fini() {
  if (certificate_) X509_free(certificate_);
  certificate_ = NULL;
  if (private_key_) EVP_PKEY_free(private_key_);
  private_key_ = NULL;
  if (!public_keys_.empty()) {
    for (unsigned i = 0; i < public_keys_.size(); ++i)
      RSA_free(public_keys_[i]);
    public_keys_.clear();
  }
  // Lookup is freed automatically
  if (x509_store_) X509_STORE_free(x509_store_);

  EVP_cleanup();

  private_key_ = NULL;
  certificate_ = NULL;
  x509_store_ = NULL;
  x509_lookup_ = NULL;
}


/**
 * OpenSSL error strings.
 */
string SignatureManager::GetCryptoError() {
  char buf[121];
  string err;
  while (ERR_peek_error() != 0) {
    ERR_error_string(ERR_get_error(), buf);
    err += string(buf);
  }
  return err;
}


/**
 * @param[in] file_pem File name of the PEM key file
 * @param[in] password Password for the private key.
 *     Password is not saved internally, but the private key is.
 * \return True on success, false otherwise
 */
bool SignatureManager::LoadPrivateKeyPath(const string &file_pem,
                                          const string &password)
{
  bool result;
  FILE *fp = NULL;
  char *tmp = strdupa(password.c_str());

  if ((fp = fopen(file_pem.c_str(), "r")) == NULL)
    return false;
  result = (private_key_ = PEM_read_PrivateKey(fp, NULL, NULL, tmp)) != NULL;
  fclose(fp);
  return result;
}


/**
 * Clears the memory storing the private key.
 */
void SignatureManager::UnloadPrivateKey() {
  if (private_key_) EVP_PKEY_free(private_key_);
  private_key_ = NULL;
}


/**
 * Loads a certificate.  This certificate is used for the following
 * signature verifications
 *
 * \return True on success, false otherwise
 */
bool SignatureManager::LoadCertificatePath(const string &file_pem) {
  if (certificate_) {
    X509_free(certificate_);
    certificate_ = NULL;
  }

  bool result;
  char *nopwd = strdupa("");
  FILE *fp;

  if ((fp = fopen(file_pem.c_str(), "r")) == NULL)
    return false;
  result = (certificate_ = PEM_read_X509_AUX(fp, NULL, NULL, nopwd)) != NULL;

  if (!result && certificate_) {
    X509_free(certificate_);
    certificate_ = NULL;
  }

  fclose(fp);
  return result;
}


/**
 * See the function that loads the certificate from file.
 */
bool SignatureManager::LoadCertificateMem(const unsigned char *buffer,
                                          const unsigned buffer_size)
{
  if (certificate_) {
    X509_free(certificate_);
    certificate_ = NULL;
  }

  bool result;
  char *nopwd = strdupa("");

  BIO *mem = BIO_new(BIO_s_mem());
  if (!mem) return false;
  if (BIO_write(mem, buffer, buffer_size) <= 0) {
    BIO_free(mem);
    return false;
  }
  result = (certificate_ = PEM_read_bio_X509_AUX(mem, NULL, NULL, nopwd))
           != NULL;
  BIO_free(mem);

  if (!result && certificate_) {
    X509_free(certificate_);
    certificate_ = NULL;
  }

  return result;
}


/**
 * Loads a list of public RSA keys separated by ":".
 */
bool SignatureManager::LoadPublicRsaKeys(const string &path_list) {
  if (!public_keys_.empty()) {
    for (unsigned i = 0; i < public_keys_.size(); ++i)
      RSA_free(public_keys_[i]);
    public_keys_.clear();
  }

  if (path_list == "")
    return true;
  const vector<string> pem_files = SplitString(path_list, ':');

  char *nopwd = strdupa("");
  FILE *fp;

  for (unsigned i = 0; i < pem_files.size(); ++i) {
    if ((fp = fopen(pem_files[i].c_str(), "r")) == NULL)
      return false;
    EVP_PKEY *this_key;
    if ((this_key = PEM_read_PUBKEY(fp, NULL, NULL, nopwd)) == NULL) {
      fclose(fp);
      return false;
    }
    fclose(fp);
    public_keys_.push_back(EVP_PKEY_get1_RSA(this_key));
    EVP_PKEY_free(this_key);
    if (public_keys_[i] == NULL)
      return false;
  }

  return true;
}


/**
 * Loads a list of blacklisted certificates (fingerprints) from a file.
 */
bool SignatureManager::LoadBlacklist(const std::string &path_blacklist) {
  blacklisted_certificates_.clear();

  char *buffer;
  unsigned buffer_size;
  if (!CopyPath2Mem(path_blacklist,
                    reinterpret_cast<unsigned char **>(&buffer), &buffer_size))
  {
    return false;
  }

  unsigned num_bytes = 0;
  while (num_bytes < buffer_size) {
    const string fingerprint = GetLineMem(buffer + num_bytes,
                                          buffer_size - num_bytes);
    blacklisted_certificates_.push_back(fingerprint);
    num_bytes += fingerprint.length() + 1;
  }
  free(buffer);

  return true;
}


vector<string> SignatureManager::GetBlacklistedCertificates() {
  return blacklisted_certificates_;
}


/**
 * Loads CA certificates CRLs from a ":" separated list of paths.
 * The information is used for proper X509 verification.
 * The format of the certificates and CRLs has to be OpenSSL hashed certs.
 * The path can be something like /etc/grid-security/certificates.
 * If path_list is empty, the default path is taken.
 */
bool SignatureManager::LoadTrustedCaCrl(const string &path_list) {
  InitX509Store();

  return true;
  /*if (path_list == "") {
    return true;
  }
  const vector<string> pem_files = SplitString(path_list, ':');*/
}


/**
 * Returns SHA-1 hash from DER encoded certificate, encoded the same way
 * OpenSSL does (01:AB:...).
 * Empty string on failure.
 */
string SignatureManager::FingerprintCertificate() {
  if (!certificate_) return "";

  int buffer_size;
  unsigned char *buffer = NULL;

  buffer_size = i2d_X509(certificate_, &buffer);
  if (buffer_size < 0) return "";

  shash::Any hash(shash::kSha1);
  shash::HashMem(buffer, buffer_size, &hash);
  free(buffer);

  const string hash_str = hash.ToString();
  string result;
  for (unsigned i = 0; i < hash_str.length(); ++i) {
    if ((i > 0) && (i%2 == 0)) result += ":";
    result += toupper(hash_str[i]);
  }
  return result;
}


/**
 * \return Some human-readable information about the loaded certificate.
 */
string SignatureManager::Whois() {
  if (!certificate_) return "No certificate loaded";

  string result;
  X509_NAME *subject = X509_get_subject_name(certificate_);
  X509_NAME *issuer = X509_get_issuer_name(certificate_);
  char *buffer = NULL;
  buffer = X509_NAME_oneline(subject, NULL, 0);
  if (buffer) {
    result = "Publisher: " + string(buffer);
    free(buffer);
  }
  buffer = X509_NAME_oneline(issuer, NULL, 0);
  if (buffer) {
    result += "\nCertificate issued by: " + string(buffer);
    free(buffer);
  }
  return result;
}


bool SignatureManager::WriteCertificateMem(unsigned char **buffer,
                                           unsigned *buffer_size)
{
  BIO *mem = BIO_new(BIO_s_mem());
  if (!mem) return false;
  if (!PEM_write_bio_X509(mem, certificate_)) {
    BIO_free(mem);
    return false;
  }

  void *bio_buffer;
  *buffer_size = BIO_get_mem_data(mem, &bio_buffer);
  *buffer = reinterpret_cast<unsigned char *>(smalloc(*buffer_size));
  memcpy(*buffer, bio_buffer, *buffer_size);
  BIO_free(mem);
  return true;
}


/**
 * Checks, whether the loaded certificate and the loaded private key match.
 *
 * \return True, if private key and certificate match, false otherwise.
 */
bool SignatureManager::KeysMatch() {
  if (!certificate_ || !private_key_)
    return false;

  bool result = false;
  const unsigned char *sign_me = reinterpret_cast<const unsigned char *>
                                   ("sign me");
  unsigned char *signature = NULL;
  unsigned signature_size;
  if (Sign(sign_me, 7, &signature, &signature_size) &&
      Verify(sign_me, 7, signature, signature_size))
  {
    result = true;
  }
  if (signature) free(signature);
  return result;
}


/**
 * Signs a data block using the loaded private key.
 *
 * \return True on sucess, false otherwise
 */
bool SignatureManager::Sign(const unsigned char *buffer,
                            const unsigned buffer_size,
                            unsigned char **signature,
                            unsigned *signature_size)
{
  if (!private_key_) {
    *signature_size = 0;
    *signature = NULL;
    return false;
  }

  bool result = false;
  EVP_MD_CTX ctx;

  *signature = reinterpret_cast<unsigned char *>(
                 smalloc(EVP_PKEY_size(private_key_)));
  EVP_MD_CTX_init(&ctx);
  if (EVP_SignInit(&ctx, EVP_sha1()) &&
      EVP_SignUpdate(&ctx, buffer, buffer_size) &&
      EVP_SignFinal(&ctx, *signature, signature_size, private_key_))
  {
    result = true;
  }
  EVP_MD_CTX_cleanup(&ctx);
  if (!result) {
    free(*signature);
    *signature_size = 0;
    *signature = NULL;
  }

  return result;
}


/**
 * Veryfies a signature against loaded certificate.
 *
 * \return True if signature is valid, false on error or otherwise
 */
bool SignatureManager::Verify(const unsigned char *buffer,
                              const unsigned buffer_size,
                              const unsigned char *signature,
                              const unsigned signature_size)
{
  if (!certificate_) return false;

  bool result = false;
  EVP_MD_CTX ctx;

  EVP_MD_CTX_init(&ctx);
  if (EVP_VerifyInit(&ctx, EVP_sha1()) &&
      EVP_VerifyUpdate(&ctx, buffer, buffer_size) &&
      EVP_VerifyFinal(&ctx, signature, signature_size,
                      X509_get_pubkey(certificate_)))
  {
    result = true;
  }
  EVP_MD_CTX_cleanup(&ctx);

  return result;
}


/**
 * Veryfies a signature against all loaded public keys.
 *
 * \return True if signature is valid with any public key, false on error or otherwise
 */
bool SignatureManager::VerifyRsa(const unsigned char *buffer,
                                 const unsigned buffer_size,
                                 const unsigned char *signature,
                                 const unsigned signature_size)
{
  for (unsigned i = 0, s = public_keys_.size(); i < s; ++i) {
    if (buffer_size > (unsigned)RSA_size(public_keys_[i]))
      continue;

    unsigned char *to = (unsigned char *)smalloc(RSA_size(public_keys_[i]));
    unsigned char *from = (unsigned char *)smalloc(signature_size);
    memcpy(from, signature, signature_size);

    int size = RSA_public_decrypt(signature_size, from, to,
                                  public_keys_[i], RSA_PKCS1_PADDING);
    free(from);
    if ((size >= 0) && (unsigned(size) == buffer_size) &&
        (memcmp(buffer, to, size) == 0))
    {
      free(to);
      return true;
    }

    free(to);
  }

  LogCvmfs(kLogSignature, kLogDebug, "VerifyRsa, no public key fits");
  return false;
}


/**
 * Strips a signature from the letter (if exists)
 */
void SignatureManager::CutLetter(const unsigned char *buffer,
                                 const unsigned buffer_size,
                                 unsigned *letter_length,
                                 unsigned *pos_after_mark)
{
  unsigned pos = 0;
  *letter_length = *pos_after_mark = 0;
  do {
    if (pos == buffer_size) {
      *pos_after_mark = pos;  // Careful: pos_after_mark points out of buffer
      *letter_length = pos;
      break;
    }

    if ((buffer[pos] == '\n') && (pos+4 <= buffer_size) &&
        (buffer[pos+1] == '-') && (buffer[pos+2] == '-') &&
        (buffer[pos+3] == '\n'))
    {
      *letter_length = pos+1;
      pos += 4;
      break;
    }
    pos++;
  } while (true);
  *pos_after_mark = pos;
}


/**
 * Checks a document of the form
 *  <ASCII LINES>
 *  --
 *  <hash>
 *  <signature>
 */
bool SignatureManager::VerifyLetter(const unsigned char *buffer,
                                    const unsigned buffer_size,
                                    const bool by_rsa)
{
  unsigned pos = 0;
  unsigned letter_length = 0;
  CutLetter(buffer, buffer_size, &letter_length, &pos);
  if (pos >= buffer_size)
    return false;

  string hash_str = "";
  unsigned hash_pos = pos;
  do {
    if (pos == buffer_size)
      return false;
    if (buffer[pos] == '\n') {
      pos++;
      break;
    }
    hash_str.push_back(buffer[pos++]);
  } while (true);
  // TODO: more hashes
  if (hash_str.length() != 2*shash::kDigestSizes[shash::kSha1])
    return false;
  shash::Any hash_printed(shash::kSha1, shash::HexPtr(hash_str));
  shash::Any hash_computed(hash_printed.algorithm);
  shash::HashMem(buffer, letter_length, &hash_computed);
  if (hash_printed != hash_computed)
    return false;

  if (by_rsa) {
    return VerifyRsa(&buffer[hash_pos], hash_str.length(),
                     &buffer[pos], buffer_size-pos);
  } else {
    return Verify(&buffer[hash_pos], hash_str.length(),
                  &buffer[pos], buffer_size-pos);
  }
}


/**
 * Verifies a detached PKCS#7 signature using the loaded trusted CAs/CRLs
 */
bool SignatureManager::VerifyLetterPkcs7(const unsigned char *buffer,
                                         const unsigned buffer_size,
                                         const unsigned char *signature,
                                         unsigned signature_size)
{
  unsigned pos_after_mark;
  unsigned letter_length;
  CutLetter(buffer, buffer_size, &letter_length, &pos_after_mark);
  if (letter_length == 0) {
    LogCvmfs(kLogSignature, kLogDebug, "empty pkcs#7 letter");
    return false;
  }

  BIO *bp_signature = BIO_new(BIO_s_mem());
  if (!bp_signature) return false;
  if (BIO_write(bp_signature, signature, signature_size) <= 0) {
    BIO_free(bp_signature);
    return false;
  }

  PKCS7 *pkcs7 = NULL;
  pkcs7 = PEM_read_bio_PKCS7(bp_signature, NULL, NULL, NULL);
  BIO_free(bp_signature);
  if (!pkcs7) {
    LogCvmfs(kLogSignature, kLogDebug, "invalid pkcs#7 signature");
    return false;
  }

  BIO *bp_content = BIO_new(BIO_s_mem());
  if (!bp_content) return false;
  if (BIO_write(bp_content, buffer, letter_length) <= 0) {
    BIO_free(bp_content);
    PKCS7_free(pkcs7);
    return false;
  }

  int flags = 0;
  STACK_OF(X509) *extra_signers = NULL;
  BIO *output_content = NULL;
  bool result = PKCS7_verify(pkcs7, extra_signers, x509_store_, bp_content,
                             output_content, flags);
  PKCS7_free(pkcs7);
  BIO_free(bp_content);
  return result == 1;
}

}  // namespace signature
