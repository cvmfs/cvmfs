/**
 * This file is part of the CernVM File System.
 *
 * This is a wrapper around OpenSSL's libcrypto.  It supports
 * signing of data with an X.509 certificate and verifying
 * a signature against a certificate.  The certificates can act only as key
 * store, in which case there is no verification against the CA chain.
 *
 * It also supports verification of plain RSA signatures (for the whitelist).
 *
 * We work exclusively with PEM formatted files (= Base64-encoded DER files).
 */


#include "crypto/signature.h"

#include <openssl/bn.h>
#include <openssl/evp.h>
#include <openssl/pkcs7.h>
#include <openssl/x509v3.h>

#include <cassert>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <unistd.h>
#include <vector>

#include "crypto/hash.h"
#include "crypto/openssl_version.h"
#include "util/concurrency.h"
#include "util/logging.h"
#include "util/platform.h"
#include "util/posix.h"
#include "util/prng.h"
#include "util/smalloc.h"
#include "util/string.h"

using namespace std;  // NOLINT

namespace signature {

const char *kDefaultPublicKey = "/etc/cvmfs/keys/cern.ch/cern-it4.pub";


static int CallbackCertVerify(int ok, X509_STORE_CTX *ctx) {
  LogCvmfs(kLogCvmfs, kLogDebug, "certificate chain verification: %d", ok);
  if (ok)
    return ok;

  const int error = X509_STORE_CTX_get_error(ctx);
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
           "certificate verification error: %s, error %s (%d)", subject.c_str(),
           X509_verify_cert_error_string(error), error);
  return ok;
}


SignatureManager::SignatureManager() {
  private_key_ = NULL;
  private_master_key_ = NULL;
  certificate_ = NULL;
  x509_store_ = NULL;
  x509_lookup_ = NULL;
  const int retval = pthread_mutex_init(&lock_blacklist_, NULL);
  assert(retval == 0);
}


void SignatureManager::InitX509Store() {
  if (x509_store_)
    X509_STORE_free(x509_store_);
  x509_lookup_ = NULL;
  x509_store_ = X509_STORE_new();
  assert(x509_store_ != NULL);

  const unsigned long verify_flags =  // NOLINT(runtime/int)
      X509_V_FLAG_CRL_CHECK | X509_V_FLAG_CRL_CHECK_ALL;
#ifdef OPENSSL_API_INTERFACE_V09
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
  UnloadCertificate();
  UnloadPrivateKey();
  UnloadPrivateMasterKey();
  UnloadPublicRsaKeys();
  // Lookup is freed automatically
  if (x509_store_)
    X509_STORE_free(x509_store_);

  EVP_cleanup();

  private_key_ = NULL;
  private_master_key_ = NULL;
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
bool SignatureManager::LoadPrivateMasterKeyPath(const string &file_pem) {
  UnloadPrivateMasterKey();
  FILE *fp;
  if ((fp = fopen(file_pem.c_str(), "r")) == NULL)
    return false;
  private_master_key_ = PEM_read_RSAPrivateKey(fp, NULL, NULL, NULL);
  fclose(fp);
  return (private_master_key_ != NULL);
}

bool SignatureManager::LoadPrivateMasterKeyMem(const string &key) {
  UnloadPrivateMasterKey();
  BIO *bp = BIO_new(BIO_s_mem());
  assert(bp != NULL);
  if (BIO_write(bp, key.data(), key.size()) <= 0) {
    BIO_free(bp);
    return false;
  }
  private_master_key_ = PEM_read_bio_RSAPrivateKey(bp, NULL, NULL, NULL);
  BIO_free(bp);
  return (private_master_key_ != NULL);
}


/**
 * @param[in] file_pem File name of the PEM key file
 * @param[in] password Password for the private key.
 *     Password is not saved internally, but the private key is.
 * \return True on success, false otherwise
 */
bool SignatureManager::LoadPrivateKeyPath(const string &file_pem,
                                          const string &password) {
  UnloadPrivateKey();
  bool result;
  FILE *fp = NULL;
  char *tmp = strdupa(password.c_str());

  if ((fp = fopen(file_pem.c_str(), "r")) == NULL)
    return false;
  result = (private_key_ = PEM_read_PrivateKey(fp, NULL, NULL, tmp)) != NULL;
  fclose(fp);
  return result;
}

bool SignatureManager::LoadPrivateKeyMem(const std::string &key) {
  UnloadPrivateKey();
  BIO *bp = BIO_new(BIO_s_mem());
  assert(bp != NULL);
  if (BIO_write(bp, key.data(), key.size()) <= 0) {
    BIO_free(bp);
    return false;
  }
  private_key_ = PEM_read_bio_PrivateKey(bp, NULL, NULL, NULL);
  BIO_free(bp);
  return (private_key_ != NULL);
}


/**
 * Clears the memory storing the private key.
 */
void SignatureManager::UnloadPrivateKey() {
  if (private_key_)
    EVP_PKEY_free(private_key_);
  private_key_ = NULL;
}


void SignatureManager::UnloadCertificate() {
  if (certificate_)
    X509_free(certificate_);
  certificate_ = NULL;
}


/**
 * Clears the memory storing the private RSA master key (whitelist signing).
 */
void SignatureManager::UnloadPrivateMasterKey() {
  if (private_master_key_)
    RSA_free(private_master_key_);
  private_master_key_ = NULL;
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
                                          const unsigned buffer_size) {
  if (certificate_) {
    X509_free(certificate_);
    certificate_ = NULL;
  }

  bool result;
  char *nopwd = strdupa("");

  BIO *mem = BIO_new(BIO_s_mem());
  if (!mem)
    return false;
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
  UnloadPublicRsaKeys();

  if (path_list == "")
    return true;
  const vector<string> pem_files = SplitString(path_list, ':');

  char *nopwd = strdupa("");
  FILE *fp;

  for (unsigned i = 0; i < pem_files.size(); ++i) {
    const char *pubkey_file = pem_files[i].c_str();

    // open public key file
    fp = fopen(pubkey_file, "r");
    if (fp == NULL) {
      LogCvmfs(kLogSignature, kLogDebug | kLogSyslogErr,
               "failed to open "
               "public key '%s'",
               pubkey_file);
      return false;
    }

    // load the public key from the file (and close it)
    EVP_PKEY *this_key = PEM_read_PUBKEY(fp, NULL, NULL, nopwd);
    fclose(fp);
    if (this_key == NULL) {
      LogCvmfs(kLogSignature, kLogDebug | kLogSyslogErr,
               "failed to load "
               "public key '%s'",
               pubkey_file);
      return false;
    }

    // read the RSA key from the loaded public key
    RSA *key = EVP_PKEY_get1_RSA(this_key);
    EVP_PKEY_free(this_key);
    if (key == NULL) {
      LogCvmfs(kLogSignature, kLogDebug | kLogSyslogErr,
               "failed to read "
               "public key '%s'",
               pubkey_file);
      return false;
    }

    // store the loaded public key
    public_keys_.push_back(key);
  }

  return true;
}


void SignatureManager::UnloadPublicRsaKeys() {
  for (unsigned i = 0; i < public_keys_.size(); ++i)
    RSA_free(public_keys_[i]);
  public_keys_.clear();
}


std::string SignatureManager::GenerateKeyText(RSA *pubkey) const {
  if (!pubkey) {
    return "";
  }

  BIO *bp = BIO_new(BIO_s_mem());
  if (bp == NULL) {
    LogCvmfs(kLogSignature, kLogDebug | kLogSyslogErr,
             "Failed to allocate"
             " memory for pubkey");
    return "";
  }
  if (!PEM_write_bio_RSA_PUBKEY(bp, pubkey)) {
    LogCvmfs(kLogSignature, kLogDebug | kLogSyslogErr,
             "Failed to write"
             " pubkey to memory");
    return "";
  }
  char *bio_pubkey_text;
  long bytes = BIO_get_mem_data(bp, &bio_pubkey_text);  // NOLINT
  std::string bio_pubkey_str(bio_pubkey_text, bytes);
  BIO_free(bp);

  return bio_pubkey_str;
}


std::string SignatureManager::GetActivePubkeys() const {
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

std::vector<std::string> SignatureManager::GetActivePubkeysAsVector() const {
  std::vector<std::string> pubkeys;
  for (std::vector<RSA *>::const_iterator it = public_keys_.begin();
       it != public_keys_.end();
       it++) {
    pubkeys.push_back(GenerateKeyText(*it));
  }
  // NOTE: we do not add the pubkey of the certificate here, as it is
  // not used for the whitelist verification.
  return pubkeys;
}

std::string SignatureManager::GetCertificate() const {
  if (!certificate_)
    return "";

  BIO *bp = BIO_new(BIO_s_mem());
  assert(bp != NULL);
  const bool rvb = PEM_write_bio_X509(bp, certificate_);
  assert(rvb);
  char *bio_crt_text;
  long bytes = BIO_get_mem_data(bp, &bio_crt_text);  // NOLINT
  assert(bytes > 0);
  std::string bio_crt_str(bio_crt_text, bytes);
  BIO_free(bp);
  return bio_crt_str;
}


std::string SignatureManager::GetPrivateKey() {
  if (!private_key_)
    return "";

  BIO *bp = BIO_new(BIO_s_mem());
  assert(bp != NULL);
  const bool rvb = PEM_write_bio_PrivateKey(bp, private_key_, NULL, NULL, 0, 0,
                                            NULL);
  assert(rvb);
  char *bio_privkey_text;
  long bytes = BIO_get_mem_data(bp, &bio_privkey_text);  // NOLINT
  assert(bytes > 0);
  std::string bio_privkey_str(bio_privkey_text, bytes);
  BIO_free(bp);
  return bio_privkey_str;
}


std::string SignatureManager::GetPrivateMasterKey() {
  if (!private_master_key_)
    return "";

  BIO *bp = BIO_new(BIO_s_mem());
  assert(bp != NULL);
  const bool rvb = PEM_write_bio_RSAPrivateKey(bp, private_master_key_, NULL,
                                               NULL, 0, 0, NULL);
  assert(rvb);
  char *bio_master_privkey_text;
  long bytes = BIO_get_mem_data(bp, &bio_master_privkey_text);  // NOLINT
  assert(bytes > 0);
  std::string bio_master_privkey_str(bio_master_privkey_text, bytes);
  BIO_free(bp);
  return bio_master_privkey_str;
}

RSA *SignatureManager::GenerateRsaKeyPair() {
  RSA *rsa = NULL;
  BIGNUM *bn = BN_new();
  int retval = BN_set_word(bn, RSA_F4);
  assert(retval == 1);
#ifdef OPENSSL_API_INTERFACE_V09
  rsa = RSA_generate_key(2048, RSA_F4, NULL, NULL);
  assert(rsa != NULL);
#else
  rsa = RSA_new();
  retval = RSA_generate_key_ex(rsa, 2048, bn, NULL);
  assert(retval == 1);
#endif
  BN_free(bn);
  return rsa;
}


/**
 * Creates the RSA master key pair for whitelist signing
 */
void SignatureManager::GenerateMasterKeyPair() {
  UnloadPrivateMasterKey();
  UnloadPublicRsaKeys();

  RSA *rsa = GenerateRsaKeyPair();
  private_master_key_ = RSAPrivateKey_dup(rsa);
  public_keys_.push_back(RSAPublicKey_dup(rsa));
  RSA_free(rsa);
}

/**
 * Creates a new RSA key pair (private key) and a self-signed certificate
 */
void SignatureManager::GenerateCertificate(const std::string &cn) {
  UnloadPrivateKey();
  UnloadCertificate();
  int retval;

  RSA *rsa = GenerateRsaKeyPair();
  private_key_ = EVP_PKEY_new();
  retval = EVP_PKEY_set1_RSA(private_key_, RSAPrivateKey_dup(rsa));
  assert(retval == 1);
  EVP_PKEY *pkey = EVP_PKEY_new();
  retval = EVP_PKEY_set1_RSA(pkey, rsa);
  assert(retval == 1);

  certificate_ = X509_new();
  X509_set_version(certificate_, 2L);
  X509_set_pubkey(certificate_, pkey);

  Prng prng;
  prng.InitLocaltime();
  unsigned long rnd_serial_no = prng.Next(uint64_t(1)
                                          + uint32_t(-1));  // NOLINT
  rnd_serial_no = rnd_serial_no
                  | uint64_t(prng.Next(uint64_t(1) + uint32_t(-1))) << 32;
  ASN1_INTEGER_set(X509_get_serialNumber(certificate_), rnd_serial_no);

  // valid as of now
  X509_gmtime_adj(
      reinterpret_cast<ASN1_TIME *>(X509_get_notBefore(certificate_)), 0);
  // valid for 1 year (validity range is unused)
  X509_gmtime_adj(
      reinterpret_cast<ASN1_TIME *>(X509_get_notAfter(certificate_)),
      3600 * 24 * 365);

  X509_NAME *name = X509_get_subject_name(certificate_);
#ifdef OPENSSL_API_INTERFACE_V09
  X509_NAME_add_entry_by_txt(
      name, "CN", MBSTRING_ASC,
      const_cast<unsigned char *>(
          reinterpret_cast<const unsigned char *>(cn.c_str())),
      -1, -1, 0);
#else
  X509_NAME_add_entry_by_txt(
      name, "CN", MBSTRING_ASC,
      reinterpret_cast<const unsigned char *>(cn.c_str()), -1, -1, 0);
#endif
  retval = X509_set_issuer_name(certificate_, name);
  assert(retval == 1);

#ifdef OPENSSL_API_INTERFACE_V09
  retval = X509_sign(certificate_, pkey, EVP_sha1());
#else
  retval = X509_sign(certificate_, pkey, EVP_sha256());
#endif
  EVP_PKEY_free(pkey);
  assert(retval > 0);
}

/**
 * Loads a list of blacklisted certificates (fingerprints) from a file.
 */
bool SignatureManager::LoadBlacklist(const std::string &path_blacklist,
                                     bool append) {
  const MutexLockGuard lock_guard(&lock_blacklist_);
  LogCvmfs(kLogSignature, kLogDebug, "reading from blacklist %s",
           path_blacklist.c_str());
  if (!append)
    blacklist_.clear();

  const int fd = open(path_blacklist.c_str(), O_RDONLY);
  if (fd < 0)
    return false;
  std::string blacklist_buffer;
  const bool retval = SafeReadToString(fd, &blacklist_buffer);
  close(fd);
  if (!retval)
    return false;

  unsigned num_bytes = 0;
  while (num_bytes < blacklist_buffer.size()) {
    const string line = GetLineMem(blacklist_buffer.data() + num_bytes,
                                   blacklist_buffer.size() - num_bytes);
    blacklist_.push_back(line);
    num_bytes += line.length() + 1;
  }

  return true;
}


vector<string> SignatureManager::GetBlacklist() {
  const MutexLockGuard lock_guard(&lock_blacklist_);
  return blacklist_;
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
    const int retval = X509_LOOKUP_add_dir(x509_lookup_, paths[i].c_str(),
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
    const shash::Algorithms hash_algorithm) {
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
    const shash::Algorithms hash_algorithm) {
  const shash::Any hash = HashCertificate(hash_algorithm);
  if (hash.IsNull())
    return "";

  const string hash_str = hash.ToString();
  string result;
  for (unsigned i = 0; i < hash_str.length(); ++i) {
    if (i < 2 * shash::kDigestSizes[hash_algorithm]) {
      if ((i > 0) && (i % 2 == 0))
        result += ":";
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
    if ((fingerprint[i] == ' ') || (fingerprint[i] == '\t')
        || (fingerprint[i] == '#')) {
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
  if (!certificate_)
    return "No certificate loaded";

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
                                           unsigned *buffer_size) {
  BIO *mem = BIO_new(BIO_s_mem());
  if (!mem)
    return false;
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
  const unsigned char *sign_me = reinterpret_cast<const unsigned char *>(
      "sign me");
  unsigned char *signature = NULL;
  unsigned signature_size;
  if (Sign(sign_me, 7, &signature, &signature_size)
      && Verify(sign_me, 7, signature, signature_size)) {
    result = true;
  }
  if (signature)
    free(signature);
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
  const bool result = X509_verify_cert(csc) == 1;
  X509_STORE_CTX_free(csc);

  return result;
}


/**
 * Signs a data block using the loaded private key.
 *
 * \return True on success, false otherwise
 */
bool SignatureManager::Sign(const unsigned char *buffer,
                            const unsigned buffer_size,
                            unsigned char **signature,
                            unsigned *signature_size) {
  if (!private_key_) {
    *signature_size = 0;
    *signature = NULL;
    return false;
  }

  bool result = false;
#ifdef OPENSSL_API_INTERFACE_V11
  EVP_MD_CTX *ctx_ptr = EVP_MD_CTX_new();
#else
  EVP_MD_CTX ctx;
  EVP_MD_CTX_init(&ctx);
  EVP_MD_CTX *ctx_ptr = &ctx;
#endif

  *signature = reinterpret_cast<unsigned char *>(
      smalloc(EVP_PKEY_size(private_key_)));
  if (EVP_SignInit(ctx_ptr, EVP_sha1())
      && EVP_SignUpdate(ctx_ptr, buffer, buffer_size)
      && EVP_SignFinal(ctx_ptr, *signature, signature_size, private_key_)) {
    result = true;
  }
#ifdef OPENSSL_API_INTERFACE_V11
  EVP_MD_CTX_free(ctx_ptr);
#else
  EVP_MD_CTX_cleanup(&ctx);
#endif
  if (!result) {
    free(*signature);
    *signature_size = 0;
    *signature = NULL;
  }

  return result;
}


/**
 * Signs a data block using the loaded private master key.
 *
 * \return True on success, false otherwise
 */
bool SignatureManager::SignRsa(const unsigned char *buffer,
                               const unsigned buffer_size,
                               unsigned char **signature,
                               unsigned *signature_size) {
  if (!private_master_key_) {
    *signature_size = 0;
    *signature = NULL;
    return false;
  }

  unsigned char *to = (unsigned char *)smalloc(RSA_size(private_master_key_));
  unsigned char *from = (unsigned char *)smalloc(buffer_size);
  memcpy(from, buffer, buffer_size);

  const int size = RSA_private_encrypt(buffer_size, from, to,
                                       private_master_key_, RSA_PKCS1_PADDING);
  free(from);
  if (size < 0) {
    *signature_size = 0;
    *signature = NULL;
    return false;
  }
  *signature = to;
  *signature_size = size;
  return true;
}


/**
 * Verifies a signature against loaded certificate.
 *
 * \return True if signature is valid, false on error or otherwise
 */
bool SignatureManager::Verify(const unsigned char *buffer,
                              const unsigned buffer_size,
                              const unsigned char *signature,
                              const unsigned signature_size) {
  if (!certificate_)
    return false;

  bool result = false;
#ifdef OPENSSL_API_INTERFACE_V11
  EVP_MD_CTX *ctx_ptr = EVP_MD_CTX_new();
#else
  EVP_MD_CTX ctx;
  EVP_MD_CTX_init(&ctx);
  EVP_MD_CTX *ctx_ptr = &ctx;
#endif

  EVP_PKEY *pubkey = X509_get_pubkey(certificate_);
  if (EVP_VerifyInit(ctx_ptr, EVP_sha1())
      && EVP_VerifyUpdate(ctx_ptr, buffer, buffer_size) &&
#ifdef OPENSSL_API_INTERFACE_V09
      EVP_VerifyFinal(ctx_ptr, const_cast<unsigned char *>(signature),
                      signature_size, pubkey)
#else
      EVP_VerifyFinal(ctx_ptr, signature, signature_size, pubkey)
#endif
  ) {
    result = true;
  }
  if (pubkey != NULL)
    EVP_PKEY_free(pubkey);
#ifdef OPENSSL_API_INTERFACE_V11
  EVP_MD_CTX_free(ctx_ptr);
#else
  EVP_MD_CTX_cleanup(&ctx);
#endif

  return result;
}


/**
 * Verifies a signature against all loaded public keys.
 *
 * \return True if signature is valid with any public key, false on error or
 * otherwise
 */
bool SignatureManager::VerifyRsa(const unsigned char *buffer,
                                 const unsigned buffer_size,
                                 const unsigned char *signature,
                                 const unsigned signature_size) {
  for (unsigned i = 0, s = public_keys_.size(); i < s; ++i) {
    if (buffer_size > (unsigned)RSA_size(public_keys_[i]))
      continue;

    unsigned char *to = (unsigned char *)smalloc(RSA_size(public_keys_[i]));
    unsigned char *from = (unsigned char *)smalloc(signature_size);
    memcpy(from, signature, signature_size);

    const int size = RSA_public_decrypt(signature_size, from, to,
                                        public_keys_[i], RSA_PKCS1_PADDING);
    free(from);
    if ((size >= 0) && (unsigned(size) == buffer_size)
        && (memcmp(buffer, to, size) == 0)) {
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
                                 unsigned *pos_after_mark) {
  unsigned pos = 0;
  *letter_length = *pos_after_mark = 0;
  do {
    if (pos == buffer_size) {
      *pos_after_mark = pos;  // Careful: pos_after_mark points out of buffer
      *letter_length = pos;
      break;
    }

    if ((buffer[pos] == '\n') && (pos + 4 <= buffer_size)
        && (buffer[pos + 1] == separator) && (buffer[pos + 2] == separator)
        && (buffer[pos + 3] == '\n')) {
      *letter_length = pos + 1;
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
                                    const bool by_rsa) {
  unsigned pos = 0;
  unsigned letter_length = 0;
  CutLetter(buffer, buffer_size, '-', &letter_length, &pos);
  if (pos >= buffer_size)
    return false;

  string hash_str = "";
  const unsigned hash_pos = pos;
  do {
    if (pos == buffer_size)
      return false;
    if (buffer[pos] == '\n') {
      pos++;
      break;
    }
    hash_str.push_back(buffer[pos++]);
  } while (true);
  const shash::Any hash_printed = shash::MkFromHexPtr(shash::HexPtr(hash_str));
  shash::Any hash_computed(hash_printed.algorithm);
  shash::HashMem(buffer, letter_length, &hash_computed);
  if (hash_printed != hash_computed)
    return false;

  if (by_rsa) {
    return VerifyRsa(&buffer[hash_pos], hash_str.length(), &buffer[pos],
                     buffer_size - pos);
  } else {
    return Verify(&buffer[hash_pos], hash_str.length(), &buffer[pos],
                  buffer_size - pos);
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
                                   vector<string> *alt_uris) {
  *content = NULL;
  *content_size = 0;

  BIO *bp_pkcs7 = BIO_new(BIO_s_mem());
  if (!bp_pkcs7)
    return false;
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

  const int flags = 0;
  STACK_OF(X509) *extra_signers = NULL;
  BIO *indata = NULL;
  const bool result = PKCS7_verify(pkcs7, extra_signers, x509_store_, indata,
                                   bp_content, flags);
  if (result != 1) {
    BIO_free(bp_content);
    PKCS7_free(pkcs7);
    return false;
  }

  BUF_MEM *bufmem_content;
  BIO_get_mem_ptr(bp_content, &bufmem_content);
  // BIO_free() leaves BUF_MEM alone
  (void)BIO_set_close(bp_content, BIO_NOCLOSE);
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
    X509 *this_signer = sk_X509_value(signers, i);
    GENERAL_NAMES *subject_alt_names = NULL;
    subject_alt_names = reinterpret_cast<GENERAL_NAMES *>(
        X509_get_ext_d2i(this_signer, NID_subject_alt_name, NULL, NULL));
    if (subject_alt_names != NULL) {
      for (int j = 0; j < sk_GENERAL_NAME_num(subject_alt_names); ++j) {
        GENERAL_NAME *this_name = sk_GENERAL_NAME_value(subject_alt_names, j);
        if (this_name->type != GEN_URI)
          continue;

        const char *name_ptr = reinterpret_cast<const char *>(
#ifdef OPENSSL_API_INTERFACE_V11
            ASN1_STRING_get0_data(this_name->d.uniformResourceIdentifier));
#else
            ASN1_STRING_data(this_name->d.uniformResourceIdentifier));
#endif
        const int name_len = ASN1_STRING_length(
            this_name->d.uniformResourceIdentifier);
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
