/**
 * This file is part of the CernVM File System.
 *
 * This is a wrapper around OpenSSL's libcrypto.  It supports
 * signing of data with an X.509 certificate and verifiying
 * a signature against a certificate.  The certificates can act only as key
 * store, in which case there is no verification against the CA chain.
 *
 * It also supports verification of plain RSA signatures (for the whitelist).
 *
 * We work exclusively with PEM formatted files (= Base64-encoded DER files).
 */

#include "cvmfs_config.h"
#include "signature.h"

#include <openssl/pkcs7.h>
#include <openssl/x509v3.h>

#include <cassert>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "compression.h"
#include "hash.h"
#include "logging.h"
#include "platform.h"
#include "smalloc.h"
#include "util.h"

using namespace std;  // NOLINT

namespace signature {

const char *kDefaultPublicKey = "/etc/cvmfs/keys/cern.ch.pub";


static int CallbackCertVerify(int ok, X509_STORE_CTX *ctx) {
  LogCvmfs(kLogCvmfs, kLogDebug, "certificate chain verification: %d", ok);
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
  x509_lookup_ = NULL;
  x509_store_ = X509_STORE_new();
  assert(x509_store_ != NULL);

  unsigned long verify_flags =  // NOLINT(runtime/int)
    X509_V_FLAG_CRL_CHECK |
    X509_V_FLAG_CRL_CHECK_ALL;
#if OPENSSL_VERSION_NUMBER < 0x00908000L
  X509_STORE_set_flags(x509_store_, verify_flags);
#else
  int retval;
  X509_VERIFY_PARAM *param = X509_VERIFY_PARAM_new();
  assert(param != NULL);
  retval = X509_VERIFY_PARAM_set_flags(param, verify_flags);
  assert(retval == 1);
  retval = X509_STORE_set1_param(x509_store_, param);
  assert(retval == 1);
  X509_VERIFY_PARAM_free(param);
#endif

  x509_lookup_ = X509_STORE_add_lookup(x509_store_, X509_LOOKUP_hash_dir());
  assert(x509_lookup_ != NULL);

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
    const char* pubkey_file = pem_files[i].c_str();

    // open public key file
    fp = fopen(pubkey_file, "r");
    if (fp == NULL) {
      LogCvmfs(kLogSignature, kLogDebug | kLogSyslogErr, "failed to open "
                                                         "public key '%s'",
               pubkey_file);
      return false;
    }

    // load the public key from the file (and close it)
    EVP_PKEY *this_key = PEM_read_PUBKEY(fp, NULL, NULL, nopwd);
    fclose(fp);
    if (this_key == NULL) {
      LogCvmfs(kLogSignature, kLogDebug | kLogSyslogErr, "failed to load "
                                                         "public key '%s'",
               pubkey_file);
      return false;
    }

    // read the RSA key from the loaded public key
    RSA *key = EVP_PKEY_get1_RSA(this_key);
    EVP_PKEY_free(this_key);
    if (key == NULL) {
      LogCvmfs(kLogSignature, kLogDebug | kLogSyslogErr, "failed to read "
                                                         "public key '%s'",
               pubkey_file);
      return false;
    }

    // store the loaded public key
    public_keys_.push_back(key);
  }

  return true;
}


std::string SignatureManager::GenerateKeyText(RSA *pubkey) {
  if (!pubkey) {return "";}

  BIO *bp = BIO_new(BIO_s_mem());
  if (bp == NULL) {
    LogCvmfs(kLogSignature, kLogDebug | kLogSyslogErr, "Failed to allocate"
             " memory for pubkey");
    return "";
  }
  if (!PEM_write_bio_RSA_PUBKEY(bp, pubkey)) {
    LogCvmfs(kLogSignature, kLogDebug | kLogSyslogErr, "Failed to write"
             " pubkey to memory");
    return "";
  }
  char *bio_pubkey_text;
  long bytes = BIO_get_mem_data(bp, &bio_pubkey_text);  // NOLINT
  std::string bio_pubkey_str(bio_pubkey_text, bytes);
  BIO_free(bp);

  return bio_pubkey_str;
}


std::string SignatureManager::GetActivePubkeys() {
  std::string pubkeys;
  for (std::vector<RSA *>::const_iterator it = public_keys_.begin();
       it != public_keys_.end();
       it++) {
    pubkeys += GenerateKeyText(*it);
  }
  // NOTE: we do not add the pubkey of the certificate here, as it is
  // not used for the whitelist verification.
  return pubkeys;
}

/**
 * Loads a list of blacklisted certificates (fingerprints) from a file.
 */
bool SignatureManager::LoadBlacklist(
  const std::string &path_blacklist,
  bool append)
{
  if (!append)
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

  /* TODO if (path_list == "") {
    return true;
  }*/
  const vector<string> paths = SplitString(path_list, ':');
  for (unsigned i = 0; i < paths.size(); ++i) {
    int retval = X509_LOOKUP_add_dir(x509_lookup_, paths[i].c_str(),
                                     X509_FILETYPE_PEM);
    if (!retval)
      return false;
  }
  return true;
}


/**
 * Returns cryptographic hash from DER encoded certificate, encoded the same way
 * OpenSSL does (01:AB:...).
 * Empty string on failure.
 */
shash::Any SignatureManager::HashCertificate(
  const shash::Algorithms hash_algorithm)
{
  shash::Any result;
  if (!certificate_)
    return result;

  int buffer_size;
  unsigned char *buffer = NULL;

  buffer_size = i2d_X509(certificate_, &buffer);
  if (buffer_size < 0)
    return result;

  result.algorithm = hash_algorithm;
  shash::HashMem(buffer, buffer_size, &result);
  free(buffer);

  return result;
}


/**
 * Returns cryptographic hash from DER encoded certificate, encoded the same way
 * OpenSSL does (01:AB:...).
 * Empty string on failure.
 */
string SignatureManager::FingerprintCertificate(
  const shash::Algorithms hash_algorithm)
{
  shash::Any hash = HashCertificate(hash_algorithm);
  if (hash.IsNull())
    return "";

  const string hash_str = hash.ToString();
  string result;
  for (unsigned i = 0; i < hash_str.length(); ++i) {
    if (i < 2*shash::kDigestSizes[hash_algorithm]) {
      if ((i > 0) && (i%2 == 0)) result += ":";
    }
    result += toupper(hash_str[i]);
  }
  return result;
}


/**
 * Parses a fingerprint from the whitelist
 */
shash::Any SignatureManager::MkFromFingerprint(const std::string &fingerprint) {
  string convert;
  for (unsigned i = 0; i < fingerprint.length(); ++i) {
    if ((fingerprint[i] == ' ') || (fingerprint[i] == '\t') ||
        (fingerprint[i] == '#'))
    {
      break;
    }
    if (fingerprint[i] != ':')
      convert.push_back(tolower(fingerprint[i]));
  }

  return shash::MkFromHexPtr(shash::HexPtr(convert));
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
 * Verifies the currently loaded certificate against the trusted CA chain.
 */
bool SignatureManager::VerifyCaChain() {
  if (!certificate_)
    return false;

  X509_STORE_CTX *csc = NULL;
  csc = X509_STORE_CTX_new();
  assert(csc);

  X509_STORE_CTX_init(csc, x509_store_, certificate_, NULL);
  bool result = X509_verify_cert(csc) == 1;
  X509_STORE_CTX_free(csc);

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
#if OPENSSL_VERSION_NUMBER < 0x00908000L
      EVP_VerifyFinal(&ctx,
                      const_cast<unsigned char *>(signature), signature_size,
                      X509_get_pubkey(certificate_))
#else
      EVP_VerifyFinal(&ctx, signature, signature_size,
                      X509_get_pubkey(certificate_))
#endif
    )
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
                                 const char separator,
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
        (buffer[pos+1] == separator) && (buffer[pos+2] == separator) &&
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
  CutLetter(buffer, buffer_size, '-', &letter_length, &pos);
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
  shash::Any hash_printed = shash::MkFromHexPtr(shash::HexPtr(hash_str));
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
 * Verifies a PKCS#7 binary content + signature structure
 * using the loaded trusted CAs/CRLs
 */
bool SignatureManager::VerifyPkcs7(const unsigned char *buffer,
                                   const unsigned buffer_size,
                                   unsigned char **content,
                                   unsigned *content_size,
                                   vector<string> *alt_uris)
{
  *content = NULL;
  *content_size = 0;

  BIO *bp_pkcs7 = BIO_new(BIO_s_mem());
  if (!bp_pkcs7) return false;
  if (BIO_write(bp_pkcs7, buffer, buffer_size) <= 0) {
    BIO_free(bp_pkcs7);
    return false;
  }

  PKCS7 *pkcs7 = NULL;
  pkcs7 = PEM_read_bio_PKCS7(bp_pkcs7, NULL, NULL, NULL);
  BIO_free(bp_pkcs7);
  if (!pkcs7) {
    LogCvmfs(kLogSignature, kLogDebug, "invalid pkcs#7 signature");
    return false;
  }

  BIO *bp_content = BIO_new(BIO_s_mem());
  if (!bp_content) {
    PKCS7_free(pkcs7);
    return false;
  }

  int flags = 0;
  STACK_OF(X509) *extra_signers = NULL;
  BIO *indata = NULL;
  bool result = PKCS7_verify(pkcs7, extra_signers, x509_store_, indata,
                             bp_content, flags);
  if (result != 1) {
    BIO_free(bp_content);
    PKCS7_free(pkcs7);
    return false;
  }

  BUF_MEM *bufmem_content;
  BIO_get_mem_ptr(bp_content, &bufmem_content);
  // BIO_free() leaves BUF_MEM alone
  (void) BIO_set_close(bp_content, BIO_NOCLOSE);
  BIO_free(bp_content);
  *content = reinterpret_cast<unsigned char *>(bufmem_content->data);
  *content_size = bufmem_content->length;
  free(bufmem_content);
  if (*content == NULL) {
    PKCS7_free(pkcs7);
    LogCvmfs(kLogSignature, kLogDebug, "empty pkcs#7 structure");
    return false;
  }

  // Extract signing certificates
  STACK_OF(X509) *signers = NULL;
  signers = PKCS7_get0_signers(pkcs7, NULL, 0);
  assert(signers);

  // Extract alternative names
  for (int i = 0; i < sk_X509_num(signers); ++i) {
    X509* this_signer = sk_X509_value(signers, i);
    GENERAL_NAMES *subject_alt_names = NULL;
    subject_alt_names = reinterpret_cast<GENERAL_NAMES *>(
      X509_get_ext_d2i(this_signer, NID_subject_alt_name, NULL, NULL));
    if (subject_alt_names != NULL) {
      for (int j = 0; j < sk_GENERAL_NAME_num(subject_alt_names); ++j) {
        GENERAL_NAME *this_name = sk_GENERAL_NAME_value(subject_alt_names, j);
        if (this_name->type != GEN_URI)
          continue;

        char *name_ptr = reinterpret_cast<char *>(
          ASN1_STRING_data(this_name->d.uniformResourceIdentifier));
        int name_len =
          ASN1_STRING_length(this_name->d.uniformResourceIdentifier);
        if (!name_ptr || (name_len <= 0))
          continue;
        alt_uris->push_back(string(name_ptr, name_len));
      }
    }
  }
  sk_X509_free(signers);
  PKCS7_free(pkcs7);
  return true;
}

}  // namespace signature
