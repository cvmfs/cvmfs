/**
 * This file is part of the CernVM File System
 */

#include "cvmfs_config.h"
#include "crypto/encrypt.h"

#include <fcntl.h>
#include <nettle/aes.h>
#include <nettle/cbc.h>
#include <unistd.h>

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <ctime>

#include "crypto/hash.h"
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
  platform_getrandom(result->data_, result->size_);
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
    return NULL;
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


Key *Key::CreateFromString(const string &key) {
  unsigned size = key.size();
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
  int fd = open(path.c_str(), O_WRONLY);
  if (fd < 0)
    return false;
  platform_disable_kcache(fd);

  int nbytes = write(fd, data_, size_);
  close(fd);
  return (nbytes >= 0) && (static_cast<unsigned>(nbytes) == size_);
}


string Key::ToBase64() const {
  return Base64(string(reinterpret_cast<const char *>(data_), size_));
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
  shash::Any hash(shash::kShake128);
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
      PANIC(NULL);
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
  if (ciphertext.length() <= kIvSize)
    return "";
  if ((ciphertext.length() % AES_BLOCK_SIZE) != 0)
    return "";

  string plaintext;
  struct CBC_CTX(struct aes256_ctx, AES_BLOCK_SIZE) cbc_ctx;

  assert(key.size() == kKeySize);
  aes256_set_decrypt_key(&cbc_ctx.ctx, key.data());

  CBC_SET_IV(&cbc_ctx, ciphertext.data());

  plaintext.resize(ciphertext.length() - kIvSize);
  assert(plaintext.length() > 0);

  CBC_DECRYPT(&cbc_ctx, aes256_decrypt, plaintext.length(),
              reinterpret_cast<uint8_t *>(plaintext.data()),
              reinterpret_cast<const uint8_t *>(ciphertext.data()) + kIvSize);

  unsigned char padding_value = plaintext[plaintext.length() - 1];
  if (padding_value > AES_BLOCK_SIZE || padding_value > plaintext.length())
    return "";
  plaintext.resize(plaintext.length() - padding_value);

  return plaintext;
}


string CipherAes256Cbc::DoEncrypt(const string &plaintext, const Key &key) {
  string ciphertext;
  struct CBC_CTX(struct aes256_ctx, AES_BLOCK_SIZE) cbc_ctx;

  assert(key.size() == kKeySize);
  aes256_set_encrypt_key(&cbc_ctx.ctx, key.data());

  // iv size happens to be md5 digest size
  shash::Md5 md5(GenerateIv(key));
  CBC_SET_IV(&cbc_ctx, md5.digest);

  // cipher length: IV + plaintext length + padding
  const size_t length_tail = plaintext.length() % AES_BLOCK_SIZE;
  const size_t length_padding = AES_BLOCK_SIZE - length_tail;
  const size_t length_cipher = AES_BLOCK_SIZE + plaintext.length() +
                               length_padding;

  ciphertext.resize(length_cipher);

  memcpy(ciphertext.data(), md5.digest, AES_BLOCK_SIZE);
  // Encrypt all full blocks of the plain text
  if (plaintext.length() / AES_BLOCK_SIZE > 0) {
    CBC_ENCRYPT(&cbc_ctx, aes256_encrypt,
                AES_BLOCK_SIZE * (plaintext.length() / AES_BLOCK_SIZE),
                reinterpret_cast<uint8_t *>(ciphertext.data()) + AES_BLOCK_SIZE,
                reinterpret_cast<const uint8_t *>(plaintext.data()));
  }

  // PKCS padding block
  unsigned char *padding_block = reinterpret_cast<uint8_t *>(ciphertext.data())
                                 + ciphertext.length() - AES_BLOCK_SIZE;
  if (length_tail > 0) {
    memcpy(padding_block, plaintext.data() + plaintext.length() - length_tail,
           length_tail);
  }
  memset(padding_block + length_tail, length_padding, length_padding);
  CBC_ENCRYPT(&cbc_ctx, aes256_encrypt, AES_BLOCK_SIZE,
              padding_block, padding_block);

  return ciphertext;
}


/**
 * The block size of AES-256-CBC happens to be the same of the MD5 digest
 * (128 bits).  Use the HMAC of a UUID to make it random and unpredictable.
 */
shash::Md5 CipherAes256Cbc::GenerateIv(const Key &key) {
  // The UUID is random but not necessarily cryptographically random.  That
  // saves the entropy pool.
  UniquePtr<cvmfs::Uuid> uuid(cvmfs::Uuid::Create(""));
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
