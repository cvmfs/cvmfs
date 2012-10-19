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
  if (certificate_) X509_free(certificate_);
  certificate_ = NULL;
  if (private_key_) EVP_PKEY_free(private_key_);
  private_key_ = NULL;
  if (!public_keys_->empty()) {
    for (unsigned i = 0; i < public_keys_->size(); ++i)
      RSA_free((*public_keys_)[i]);
    public_keys_->clear();
  }
  EVP_cleanup();
  delete public_keys_;
  delete blacklisted_certificates_;
  private_key_ = NULL;
  certificate_ = NULL;
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
    const string fingerprint = GetLineMem(buffer + num_bytes,
                                          buffer_size - num_bytes);
    blacklisted_certificates_->push_back(fingerprint);
    num_bytes += fingerprint.length() + 1;
  }
  free(buffer);

  return true;
}


vector<string> GetBlacklistedCertificates() {
  if (blacklisted_certificates_)
    return *blacklisted_certificates_;
  return vector<string>();
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
 * Checks a document of the form
 *  <ASCII LINES>
 *  --
 *  <hash>
 *  <signature>
 */
bool VerifyLetter(const unsigned char *buffer, const unsigned buffer_size,
                  const bool by_rsa)
{
  unsigned pos = 0;
  unsigned letter_length = 0;
  do {
    if (pos > buffer_size-3)
      return false;
    if ((buffer[pos] == '-') && (buffer[pos+1] == '-') &&
        (buffer[pos+2] == '\n'))
    {
      letter_length = pos;
      pos += 3;
      break;
    }
    pos++;
  } while (true);

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
  if (hash_str.length() != 2*hash::kDigestSizes[hash::kSha1])
    return false;
  hash::Any hash_printed(hash::kSha1, hash::HexPtr(hash_str));
  hash::Any hash_computed(hash_printed.algorithm);
  hash::HashMem(buffer, letter_length, &hash_computed);
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

}  // namespace signature
