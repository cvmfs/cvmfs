/**
 * This file is part of the CernVM File System
 */

#include "cvmfs_config.h"
#include "encrypt.h"

#include <fcntl.h>
#include <openssl/evp.h>
#include <openssl/rand.h>

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <ctime>

#include "hash.h"
#include "platform.h"
#include "smalloc.h"
#include "util.h"
#include "util_concurrency.h"

using namespace std;  // NOLINT

namespace cipher {

Key *Key::CreateRandomly(const unsigned size) {
  Key *result = new Key();
  result->size_ = size;
  result->data_ = reinterpret_cast<unsigned char *>(smalloc(size));
  // TODO(jblomer): pin memory in RAM
  int retval = RAND_bytes(result->data_, result->size_);
  if (retval != 1) {
    // Not enough entropy
    delete result;
    result = NULL;
  }
  return result;
}


Key *Key::CreateFromFile(const string &path) {
  int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0)
    return NULL;
  platform_disable_kcache(fd);

  platform_stat64 info;
  int retval = platform_fstat(fd, &info);
  if (retval != 0) {
    close(fd);
    return NULL;
  }
  if ((info.st_size == 0) || (info.st_size > kMaxSize)) {
    close(fd);
    return false;
  }

  Key *result = new Key();
  result->size_ = info.st_size;
  result->data_ = reinterpret_cast<unsigned char *>(smalloc(result->size_));
  int nbytes = read(fd, result->data_, result->size_);
  close(fd);
  if ((nbytes < 0) || (static_cast<unsigned>(nbytes) != result->size_)) {
    delete result;
    result = NULL;
  }
  return result;
}


Key::~Key() {
  if (data_) {
    memset(data_, 0, size_);
    free(data_);
  }
}


bool Key::SaveToFile(const std::string &path) {
  int fd = open(path.c_str(), O_WRONLY);
  if (fd < 0)
    return false;
  platform_disable_kcache(fd);

  int nbytes = write(fd, data_, size_);
  close(fd);
  return (nbytes >= 0) && (static_cast<unsigned>(nbytes) == size_);
}


//------------------------------------------------------------------------------


MemoryKeyDatabase::MemoryKeyDatabase() {
  lock_ =
    reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock_, NULL);
  assert(retval == 0);
}


MemoryKeyDatabase::~MemoryKeyDatabase() {
  pthread_mutex_destroy(lock_);
  free(lock_);
}


bool MemoryKeyDatabase::StoreNew(const Key *key, string *id) {
  MutexLockGuard mutex_guard(lock_);
  // TODO(jblomer): is this good enough for random keys? Salting? KDF2?
  shash::Any hash(shash::kSha256);
  HashMem(key->data(), key->size(), &hash);
  *id = "H" + hash.ToString();
  map<string, const Key *>::const_iterator i = database_.find(*id);
  if (i != database_.end())
    return false;

  database_[*id] = key;
  return true;
}


const Key *MemoryKeyDatabase::Find(const string &id) {
  MutexLockGuard mutex_guard(lock_);
  map<string, const Key *>::const_iterator i = database_.find(id);
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
      abort();
  }
  // Never here
}


bool Cipher::Encrypt(
  const string &plaintext,
  const Key &key,
  string *ciphertext)
{
  ciphertext->clear();
  if (key.size() != key_size())
    return false;

  unsigned char envelope = 0 & 0x0F;
  envelope |= (algorithm() << 4) & 0xF0;
  ciphertext->push_back(envelope);

  *ciphertext += DoEncrypt(plaintext, key);
  return true;
}


bool Cipher::Decrypt(
  const string &ciphertext,
  const Key &key,
  string *plaintext)
{
  plaintext->clear();
  if (ciphertext.size() < 1)
    return false;
  unsigned char envelope = ciphertext[0];
  unsigned char version = envelope & 0x0F;
  if (version != 0)
    return false;
  unsigned char algorithm = (envelope & 0xF0) >> 4;
  if (algorithm > kNone)
    return false;

  UniquePtr<Cipher> cipher(Create(static_cast<Algorithms>(algorithm)));
  if (key.size() != cipher->key_size())
    return false;
  *plaintext += cipher->DoDecrypt(ciphertext.substr(1), key);
  return true;
}


//------------------------------------------------------------------------------


string CipherAes256Cbc::DoDecrypt(const string &ciphertext, const Key &key) {
  assert(key.size() == kKeySize);
  int retval, retval_2;
  if (ciphertext.size() < kIvSize)
    return "";

  const unsigned char *iv = reinterpret_cast<const unsigned char *>(
    ciphertext.data());

  // See OpenSSL documentation for the size
  unsigned char *plaintext = reinterpret_cast<unsigned char *>(
    smalloc(kBlockSize + ciphertext.size() - kIvSize));
  int plaintext_len;
  EVP_CIPHER_CTX ctx;
  EVP_CIPHER_CTX_init(&ctx);
  retval = EVP_DecryptInit_ex(&ctx, EVP_aes_256_cbc(), NULL, key.data(), iv);
  assert(retval == 1);
  retval = EVP_DecryptUpdate(&ctx,
             plaintext, &plaintext_len,
             reinterpret_cast<const unsigned char *>(
               ciphertext.data() + kIvSize),
             ciphertext.length() - kIvSize);
  if (retval != 1) {
    free(plaintext);
    retval = EVP_CIPHER_CTX_cleanup(&ctx);
    assert(retval == 1);
    return "";
  }
  retval = EVP_DecryptFinal_ex(&ctx, plaintext + plaintext_len, &plaintext_len);
  retval_2 = EVP_CIPHER_CTX_cleanup(&ctx);
  assert(retval_2 == 1);
  if (retval != 1) {
    free(plaintext);
    return "";
  }

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

  shash::Md5 md5(GenerateIv());
  // iv size happens to be md5 digest size
  unsigned char *iv = md5.digest;

  // See OpenSSL documentation as for the size.  Additionally, we prepend the
  // initialization vector.
  unsigned char *ciphertext = reinterpret_cast<unsigned char *>(
    smalloc(kIvSize + 2 * kBlockSize + plaintext.size()));
  memcpy(ciphertext, iv, kIvSize);
  int cipher_len;
  EVP_CIPHER_CTX ctx;
  EVP_CIPHER_CTX_init(&ctx);
  retval = EVP_EncryptInit_ex(&ctx, EVP_aes_256_cbc(), NULL, key.data(), iv);
  assert(retval == 1);
  retval = EVP_EncryptUpdate(&ctx,
             ciphertext + kIvSize, &cipher_len,
             reinterpret_cast<const unsigned char *>(plaintext.data()),
             plaintext.length());
  assert(retval == 1);
  retval = EVP_EncryptFinal_ex(&ctx, ciphertext + kIvSize + cipher_len,
                               &cipher_len);
  assert(retval == 1);
  retval = EVP_CIPHER_CTX_cleanup(&ctx);
  assert(retval == 1);

  assert(cipher_len > 0);
  string result(reinterpret_cast<char *>(ciphertext), kIvSize + cipher_len);
  free(ciphertext);
  return result;
}


/**
 * The block size of AES-256-CBC happens to be the same of the MD5 digest
 * (128 bits)
 */
shash::Md5 CipherAes256Cbc::GenerateIv() {
  // use hash over real time and monotonic time
  struct timespec time_stamp[2];
  int retval = clock_gettime(CLOCK_REALTIME, &(time_stamp[0]));
  assert(retval == 0);
  retval = clock_gettime(CLOCK_MONOTONIC, &(time_stamp[1]));
  assert(retval == 0);
  return shash::Md5(reinterpret_cast<const char *>(&time_stamp),
                    2 * sizeof(struct timespec));
}


//------------------------------------------------------------------------------


string CipherNone::DoDecrypt(const string &ciphertext, const Key &key) {
  return ciphertext;
}


string CipherNone::DoEncrypt(const string &plaintext, const Key &key) {
  return plaintext;
}

}  // namespace cipher
