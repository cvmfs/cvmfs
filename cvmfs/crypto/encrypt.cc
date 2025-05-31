/**
 * This file is part of the CernVM File System
 */


#include "crypto/encrypt.h"

#include <fcntl.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <unistd.h>

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <ctime>

#include "crypto/hash.h"
#include "crypto/openssl_version.h"
#include "util/concurrency.h"
#include "util/exception.h"
#include "util/platform.h"
#include "util/pointer.h"
#include "util/smalloc.h"
#include "util/string.h"
#include "util/uuid.h"

using namespace std;  // NOLINT

namespace cipher {

Key *Key::CreateRandomly(const unsigned size) {
  Key *result = new Key();
  result->size_ = size;
  result->data_ = reinterpret_cast<unsigned char *>(smalloc(size));
  // TODO(jblomer): pin memory in RAM
  const int retval = RAND_bytes(result->data_, result->size_);
  if (retval != 1) {
    // Not enough entropy
    delete result;
    result = NULL;
  }
  return result;
}


Key *Key::CreateFromFile(const string &path) {
  const int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0)
    return NULL;
  platform_disable_kcache(fd);

  platform_stat64 info;
  const int retval = platform_fstat(fd, &info);
  if (retval != 0) {
    close(fd);
    return NULL;
  }
  if ((info.st_size == 0) || (info.st_size > kMaxSize)) {
    close(fd);
    return NULL;
  }

  Key *result = new Key();
  result->size_ = info.st_size;
  result->data_ = reinterpret_cast<unsigned char *>(smalloc(result->size_));
  const int nbytes = read(fd, result->data_, result->size_);
  close(fd);
  if ((nbytes < 0) || (static_cast<unsigned>(nbytes) != result->size_)) {
    delete result;
    result = NULL;
  }
  return result;
}


Key *Key::CreateFromString(const string &key) {
  const unsigned size = key.size();
  if ((size == 0) || (size > kMaxSize))
    return NULL;
  UniquePtr<Key> result(new Key());
  result->size_ = size;
  result->data_ = reinterpret_cast<unsigned char *>(smalloc(size));
  memcpy(result->data_, key.data(), size);
  return result.Release();
}


Key::~Key() {
  if (data_) {
    memset(data_, 0, size_);
    free(data_);
  }
}


bool Key::SaveToFile(const std::string &path) {
  const int fd = open(path.c_str(), O_WRONLY);
  if (fd < 0)
    return false;
  platform_disable_kcache(fd);

  const int nbytes = write(fd, data_, size_);
  close(fd);
  return (nbytes >= 0) && (static_cast<unsigned>(nbytes) == size_);
}


string Key::ToBase64() const {
  return Base64(string(reinterpret_cast<const char *>(data_), size_));
}


//------------------------------------------------------------------------------


MemoryKeyDatabase::MemoryKeyDatabase() {
  lock_ = reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  const int retval = pthread_mutex_init(lock_, NULL);
  assert(retval == 0);
}


MemoryKeyDatabase::~MemoryKeyDatabase() {
  pthread_mutex_destroy(lock_);
  free(lock_);
}


bool MemoryKeyDatabase::StoreNew(const Key *key, string *id) {
  const MutexLockGuard mutex_guard(lock_);
  // TODO(jblomer): is this good enough for random keys? Salting? KDF2?
  shash::Any hash(shash::kShake128);
  HashMem(key->data(), key->size(), &hash);
  *id = "H" + hash.ToString();
  const map<string, const Key *>::const_iterator i = database_.find(*id);
  if (i != database_.end())
    return false;

  database_[*id] = key;
  return true;
}


const Key *MemoryKeyDatabase::Find(const string &id) {
  const MutexLockGuard mutex_guard(lock_);
  const map<string, const Key *>::const_iterator i = database_.find(id);
  if (i != database_.end())
    return i->second;
  return NULL;
}


//------------------------------------------------------------------------------


Cipher *Cipher::Create(const Algorithms a) {
  switch (a) {
    case kAes256Cbc:
      return new CipherAes256Cbc();
    case kNone:
      return new CipherNone();
    default:
      PANIC(NULL);
  }
  // Never here
}


bool Cipher::Encrypt(const string &plaintext,
                     const Key &key,
                     string *ciphertext) {
  ciphertext->clear();
  if (key.size() != key_size())
    return false;

  unsigned char envelope = 0 & 0x0F;
  envelope |= (algorithm() << 4) & 0xF0;
  ciphertext->push_back(envelope);

  *ciphertext += DoEncrypt(plaintext, key);
  return true;
}


bool Cipher::Decrypt(const string &ciphertext,
                     const Key &key,
                     string *plaintext) {
  plaintext->clear();
  if (ciphertext.size() < 1)
    return false;
  const unsigned char envelope = ciphertext[0];
  const unsigned char version = envelope & 0x0F;
  if (version != 0)
    return false;
  const unsigned char algorithm = (envelope & 0xF0) >> 4;
  if (algorithm > kNone)
    return false;

  const UniquePtr<Cipher> cipher(Create(static_cast<Algorithms>(algorithm)));
  if (key.size() != cipher->key_size())
    return false;
  *plaintext += cipher->DoDecrypt(ciphertext.substr(1), key);
  return true;
}


//------------------------------------------------------------------------------


string CipherAes256Cbc::DoDecrypt(const string &ciphertext, const Key &key) {
  assert(key.size() == kKeySize);
  int retval;
  if (ciphertext.size() < kIvSize)
    return "";

  const unsigned char *iv = reinterpret_cast<const unsigned char *>(
      ciphertext.data());

  // See OpenSSL documentation for the size
  unsigned char *plaintext = reinterpret_cast<unsigned char *>(
      smalloc(kBlockSize + ciphertext.size() - kIvSize));
  int plaintext_len;
  int tail_len;
#ifdef OPENSSL_API_INTERFACE_V11
  EVP_CIPHER_CTX *ctx_ptr = EVP_CIPHER_CTX_new();
#else
  EVP_CIPHER_CTX ctx;
  EVP_CIPHER_CTX_init(&ctx);
  EVP_CIPHER_CTX *ctx_ptr = &ctx;
#endif
  retval = EVP_DecryptInit_ex(ctx_ptr, EVP_aes_256_cbc(), NULL, key.data(), iv);
  assert(retval == 1);
  retval = EVP_DecryptUpdate(
      ctx_ptr, plaintext, &plaintext_len,
      reinterpret_cast<const unsigned char *>(ciphertext.data() + kIvSize),
      ciphertext.length() - kIvSize);
  if (retval != 1) {
    free(plaintext);
#ifdef OPENSSL_API_INTERFACE_V11
    EVP_CIPHER_CTX_free(ctx_ptr);
#else
    retval = EVP_CIPHER_CTX_cleanup(&ctx);
    assert(retval == 1);
#endif
    return "";
  }
  retval = EVP_DecryptFinal_ex(ctx_ptr, plaintext + plaintext_len, &tail_len);
#ifdef OPENSSL_API_INTERFACE_V11
  EVP_CIPHER_CTX_free(ctx_ptr);
#else
  int retval_2 = EVP_CIPHER_CTX_cleanup(&ctx);
  assert(retval_2 == 1);
#endif
  if (retval != 1) {
    free(plaintext);
    return "";
  }

  plaintext_len += tail_len;
  if (plaintext_len == 0) {
    free(plaintext);
    return "";
  }
  string result(reinterpret_cast<char *>(plaintext), plaintext_len);
  free(plaintext);
  return result;
}


string CipherAes256Cbc::DoEncrypt(const string &plaintext, const Key &key) {
  assert(key.size() == kKeySize);
  int retval;

  shash::Md5 md5(GenerateIv(key));
  // iv size happens to be md5 digest size
  unsigned char *iv = md5.digest;

  // See OpenSSL documentation as for the size.  Additionally, we prepend the
  // initialization vector.
  unsigned char *ciphertext = reinterpret_cast<unsigned char *>(
      smalloc(kIvSize + 2 * kBlockSize + plaintext.size()));
  memcpy(ciphertext, iv, kIvSize);
  int cipher_len = 0;
  int tail_len = 0;
#ifdef OPENSSL_API_INTERFACE_V11
  EVP_CIPHER_CTX *ctx_ptr = EVP_CIPHER_CTX_new();
#else
  EVP_CIPHER_CTX ctx;
  EVP_CIPHER_CTX_init(&ctx);
  EVP_CIPHER_CTX *ctx_ptr = &ctx;
#endif
  retval = EVP_EncryptInit_ex(ctx_ptr, EVP_aes_256_cbc(), NULL, key.data(), iv);
  assert(retval == 1);
  // Older versions of OpenSSL don't allow empty input buffers
  if (!plaintext.empty()) {
    retval = EVP_EncryptUpdate(
        ctx_ptr, ciphertext + kIvSize, &cipher_len,
        reinterpret_cast<const unsigned char *>(plaintext.data()),
        plaintext.length());
    assert(retval == 1);
  }
  retval = EVP_EncryptFinal_ex(ctx_ptr, ciphertext + kIvSize + cipher_len,
                               &tail_len);
  assert(retval == 1);
#ifdef OPENSSL_API_INTERFACE_V11
  EVP_CIPHER_CTX_free(ctx_ptr);
#else
  retval = EVP_CIPHER_CTX_cleanup(&ctx);
  assert(retval == 1);
#endif

  cipher_len += tail_len;
  assert(cipher_len > 0);
  string result(reinterpret_cast<char *>(ciphertext), kIvSize + cipher_len);
  free(ciphertext);
  return result;
}


/**
 * The block size of AES-256-CBC happens to be the same of the MD5 digest
 * (128 bits).  Use the HMAC of a UUID to make it random and unpredictable.
 */
shash::Md5 CipherAes256Cbc::GenerateIv(const Key &key) {
  // The UUID is random but not necessarily cryptographically random.  That
  // saves the entropy pool.
  const UniquePtr<cvmfs::Uuid> uuid(cvmfs::Uuid::Create(""));
  assert(uuid.IsValid());

  // Now make it unpredictable, using an HMAC with the encryption key.
  shash::Any hmac(shash::kMd5);
  shash::Hmac(string(reinterpret_cast<const char *>(key.data()), key.size()),
              uuid->data(), uuid->size(), &hmac);
  return hmac.CastToMd5();
}


//------------------------------------------------------------------------------


string CipherNone::DoDecrypt(const string &ciphertext, const Key &key) {
  return ciphertext;
}


string CipherNone::DoEncrypt(const string &plaintext, const Key &key) {
  return plaintext;
}

}  // namespace cipher
