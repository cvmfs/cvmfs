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

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/err.h>
#include <openssl/bio.h>
#include <openssl/rsa.h>
#include <openssl/engine.h>

#include <string>
#include <vector>

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cctype>

#include "platform.h"
#include "hash.h"
#include "util.h"
#include "smalloc.h"
#include "compression.h"

using namespace std;  // NOLINT

namespace signature {

const char *kDefaultPublicKey = "/etc/cvmfs/keys/cern.ch.pub";

EVP_PKEY *private_key_ = NULL;
X509 *certificate_ = NULL;
vector<RSA *> *public_keys_;  /**< Contains cvmfs public master keys */
vector<string> *blacklisted_certificates_ = NULL;

void Init() {
  OpenSSL_add_all_algorithms();
  public_keys_ = new vector<RSA *>();
  blacklisted_certificates_ = new vector<string>();
}


void Fini() {
  EVP_cleanup();
  if (certificate_) X509_free(certificate_);
  if (private_key_) EVP_PKEY_free(private_key_);
  if (!public_keys_->empty()) {
    for (unsigned i = 0; i < public_keys_->size(); ++i)
      RSA_free((*public_keys_)[i]);
    public_keys_->clear();
  }
  delete public_keys_;
  delete blacklisted_certificates_;
  public_keys_ = NULL;
  blacklisted_certificates_ = NULL;
}


/**
 * OpenSSL error strings.
 */
string GetCryptoError() {
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
bool LoadPrivateKeyPath(const string &file_pem, const string &password) {
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
void UnloadPrivateKey() {
  if (private_key_) EVP_PKEY_free(private_key_);
  private_key_ = NULL;
}


/**
 * Loads a certificate.  This certificate is used for the following
 * signature verifications
 *
 * \return True on success, false otherwise
 */
bool LoadCertificatePath(const string &file_pem) {
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
bool LoadCertificateMem(const unsigned char *buffer,
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
bool LoadPublicRsaKeys(const string &path_list) {
  if (!public_keys_->empty()) {
    for (unsigned i = 0; i < public_keys_->size(); ++i)
      RSA_free((*public_keys_)[i]);
    public_keys_->clear();
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
    public_keys_->push_back(EVP_PKEY_get1_RSA(this_key));
    EVP_PKEY_free(this_key);
    if ((*public_keys_)[i] == NULL)
      return false;
  }

  return true;
}


/**
 * Loads a list of blacklisted certificates (fingerprints) from a file.
 */
bool LoadBlacklist(const std::string &path_blacklist) {
  blacklisted_certificates_->clear();

  char *buffer;
  unsigned buffer_size;
  if (!CopyPath2Mem(path_blacklist,
                    reinterpret_cast<unsigned char **>(&buffer), &buffer_size))
  {
    return false;
  }

  unsigned num_bytes = 0;
  while (num_bytes < buffer_size) {
    const string fingerprint = GetLine(buffer + num_bytes,
                                       buffer_size - num_bytes);
    blacklisted_certificates_->push_back(fingerprint);
    num_bytes += fingerprint.length() + 1;
  }
  free(buffer);

  return true;
}


/**
 * Returns SHA-1 hash from DER encoded certificate, encoded the same way
 * OpenSSL does (01:AB:...).
 * Empty string on failure.
 */
string FingerprintCertificate() {
  if (!certificate_) return "";

  int buffer_size;
  unsigned char *buffer = NULL;

  buffer_size = i2d_X509(certificate_, &buffer);
  if (buffer_size < 0) return "";

  hash::Any hash(hash::kSha1);
  hash::HashMem(buffer, buffer_size, &hash);
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
string Whois() {
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


bool WriteCertificateMem(unsigned char **buffer, unsigned *buffer_size) {
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
bool KeysMatch() {
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
bool Sign(const unsigned char *buffer, const unsigned buffer_size,
          unsigned char **signature, unsigned *signature_size)
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
bool Verify(const unsigned char *buffer, const unsigned buffer_size,
            const unsigned char *signature, const unsigned signature_size)
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
bool VerifyRsa(const unsigned char *buffer, const unsigned buffer_size,
               const unsigned char *signature, const unsigned signature_size)
{
  for (unsigned i = 0, s = public_keys_->size(); i < s; ++i) {
    if (buffer_size > (unsigned)RSA_size((*public_keys_)[i]))
      continue;

    unsigned char *to = (unsigned char *)smalloc(RSA_size((*public_keys_)[i]));
    unsigned char *from = (unsigned char *)smalloc(signature_size);
    memcpy(from, signature, signature_size);

    int size = RSA_public_decrypt(signature_size, from, to,
                                  (*public_keys_)[i], RSA_PKCS1_PADDING);
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
 * Reads after skip bytes in memory, looks for a line break and saves
 * the rest into sig_buf, which will be allocated.
 */
bool ReadSignatureTail(const unsigned char *buffer, const unsigned buffer_size,
                       const unsigned skip_bytes,
                       unsigned char **signature, unsigned *signature_size)
{
  unsigned i;
  for (i = skip_bytes; i < buffer_size; ++i) {
    if (((char *)buffer)[i] == '\n') break;
  }
  i++;
  /* at least one byte after \n required */
  if (i >= buffer_size) {
    *signature = NULL;
    *signature_size = 0;
    return false;
  } else {
    *signature_size = buffer_size-i;
    *signature = reinterpret_cast<unsigned char *>(smalloc(*signature_size));
    memcpy(*signature, ((char *)buffer)+i, *signature_size);
    return true;
  }
}


/**
 * Checks whether the fingerprint of the loaded PEM certificate is listed on the
 * whitelist stored in a memory chunk.
 */
bool VerifyWhitelist(const char *whitelist, const unsigned whitelist_size,
                     const string &expected_repository)
{
  const string fingerprint = FingerprintCertificate();
  if (fingerprint == "") {
    LogCvmfs(kLogSignature, kLogDebug, "invalid fingerprint");
    return false;
  }
  LogCvmfs(kLogSignature, kLogDebug,
           "checking certificate with fingerprint %s against whitelist",
           fingerprint.c_str());

  time_t local_timestamp = time(NULL);
  string line;
  unsigned payload_bytes = 0;

  // Check timestamp (UTC), ignore issue date (legacy)
  line = GetLine(whitelist, whitelist_size);
  if (line.length() != 14) {
    LogCvmfs(kLogSignature, kLogDebug, "invalid timestamp format");
    return false;
  }
  payload_bytes += 15;

  // Expiry date
  line = GetLine(whitelist+payload_bytes, whitelist_size-payload_bytes);
  if (line.length() != 15) {
    LogCvmfs(kLogSignature, kLogDebug, "invalid timestamp format");
    return false;
  }
  struct tm tm_wl;
  memset(&tm_wl, 0, sizeof(struct tm));
  tm_wl.tm_year = String2Int64(line.substr(1, 4))-1900;
  tm_wl.tm_mon = String2Int64(line.substr(5, 2)) - 1;
  tm_wl.tm_mday = String2Int64(line.substr(7, 2));
  tm_wl.tm_hour = String2Int64(line.substr(9, 2));
  tm_wl.tm_min = tm_wl.tm_sec = 0;  // exact on hours level
  time_t timestamp = timegm(&tm_wl);
  LogCvmfs(kLogSignature, kLogDebug,
           "whitelist UTC expiry timestamp in localtime: %s",
           StringifyTime(timestamp, false).c_str());
  if (timestamp < 0) {
    LogCvmfs(kLogSignature, kLogDebug, "invalid timestamp");
    return false;
  }
  LogCvmfs(kLogSignature, kLogDebug,  "local time: %s",
           StringifyTime(local_timestamp, true).c_str());
  if (local_timestamp > timestamp) {
    LogCvmfs(kLogSignature, kLogDebug,
             "whitelist lifetime verification failed, expired");
    return false;
  }
  payload_bytes += 16;

  // Check repository name
  line = GetLine(whitelist+payload_bytes, whitelist_size-payload_bytes);
  if ((expected_repository != "") && ("N" + expected_repository != line)) {
    LogCvmfs(kLogSignature, kLogDebug,
             "repository name does not match (found %s, expected %s)",
             line.c_str(), expected_repository.c_str());
    return false;
  }
  payload_bytes += line.length() + 1;

  // Search the fingerprint
  bool found = false;
  do {
    line = GetLine(whitelist+payload_bytes, whitelist_size-payload_bytes);
    if (line == "--") break;
    if (line.substr(0, 59) == fingerprint)
      found = true;
    payload_bytes += line.length() + 1;
  } while (payload_bytes < whitelist_size);
  payload_bytes += line.length() + 1;

  if (!found) {
    LogCvmfs(kLogSignature, kLogDebug,
             "the certificate's fingerprint is not on the whitelist");
    return false;
  }

  // Check whitelist signature
  line = GetLine(whitelist+payload_bytes, whitelist_size-payload_bytes);
  if (line.length() < 40) {
    LogCvmfs(kLogSignature, kLogDebug,
             "no checksum at the end of whitelist found");
    return false;
  }
  hash::Any hash(hash::kSha1, hash::HexPtr(line.substr(0, 40)));
  hash::Any compare(hash::kSha1);
  hash::HashMem((const unsigned char *)whitelist, payload_bytes-3, &compare);
  if (hash != compare) {
    LogCvmfs(kLogSignature, kLogDebug, "whitelist checksum does not match");
    return false;
  }

  // Check local blacklist
  for (unsigned i = 0; i < blacklisted_certificates_->size(); ++i) {
    if ((*blacklisted_certificates_)[i].substr(0, 59) == fingerprint) {
      LogCvmfs(kLogSignature, kLogDebug | kLogSyslog,
               "blacklisted fingerprint (%s)", fingerprint.c_str());
      return false;
    }
  }

  // Verify signature
  unsigned char *signature;
  unsigned signature_size;
  if (!ReadSignatureTail((const unsigned char *)whitelist, whitelist_size,
                         payload_bytes, &signature, &signature_size))
  {
    LogCvmfs(kLogSignature, kLogDebug,
             "no signature at the end of whitelist found");
    return false;
  }
  const string hash_str = hash.ToString();
  bool result = VerifyRsa((const unsigned char *)&hash_str[0],
                          hash_str.length(), signature, signature_size);
  free(signature);
  if (!result)
    LogCvmfs(kLogSignature, kLogDebug,
             "whitelist signature verification failed (hash %s), %s",
             hash_str.c_str(), GetCryptoError().c_str());
  else
    LogCvmfs(kLogSignature, kLogDebug, "whitelist signature verification passed");

  return result;
}

}  // namespace signature
