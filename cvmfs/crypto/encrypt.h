/**
 * This file is part of the CernVM File System
 *
 * Symmetric encryption/decryption of small pieces of text.  Plain text and
 * cipher text are stored in std::string.  Uses cbc mode, there is no inherent
 * integrity check.  The cipher text should only be used in a data structure
 * that is itself protected by a digital signature, HMAC, or similar.
 *
 * The initialization vector is transferred together with the cipher text.  It
 * is constructed from the HMAC of the key and nonce.
 */

#ifndef CVMFS_CRYPTO_ENCRYPT_H_
#define CVMFS_CRYPTO_ENCRYPT_H_

#include <pthread.h>

#include <map>
#include <string>

#include "crypto/hash.h"
#include "duplex_testing.h"
#include "util/export.h"
#include "util/single_copy.h"

namespace cipher {

enum Algorithms {
  kAes256Cbc = 0,
  kNone,  // needs to be last
};


/**
 * Encapsulates a small non-copyable piece of pinned memory.  Is actively set to
 * zero on destruction.
 */
class CVMFS_EXPORT Key : SingleCopy {
 public:
  static const unsigned kMaxSize = 64;

  static Key *CreateRandomly(const unsigned size);
  static Key *CreateFromFile(const std::string &path);
  static Key *CreateFromString(const std::string &key);
  bool SaveToFile(const std::string &path);
  ~Key();

  unsigned size() const { return size_; }
  const unsigned char *data() const { return data_; }
  std::string ToBase64() const;

 private:
  Key() : data_(NULL), size_(0) { }
  unsigned char *data_;
  unsigned size_;
};


/**
 * Allows to access keys by identifiers.  This might at some point move into a
 * separate compilation unit.
 */
class CVMFS_EXPORT AbstractKeyDatabase {
 public:
  virtual ~AbstractKeyDatabase() { }
  virtual bool StoreNew(const Key *key, std::string *id) = 0;
  virtual const Key *Find(const std::string &id) = 0;
};


class CVMFS_EXPORT MemoryKeyDatabase : SingleCopy, public AbstractKeyDatabase {
 public:
  MemoryKeyDatabase();
  virtual ~MemoryKeyDatabase();
  virtual bool StoreNew(const Key *key, std::string *id);
  virtual const Key *Find(const std::string &id);

 private:
  pthread_mutex_t *lock_;
  std::map<std::string, const Key *> database_;
};


/**
 * The interface for an encryption algorithm.  Uses a simple envelope with a one
 * byte prefix: the first 4 bits are the envelope version (currently 0), the
 * second 4 bits are the encryption algorithm and refers to Algorithms.
 */
class CVMFS_EXPORT Cipher {
 public:
  static Cipher *Create(const Algorithms a);
  virtual ~Cipher() { }

  bool Encrypt(const std::string &plaintext, const Key &key,
               std::string *ciphertext);
  static bool Decrypt(const std::string &ciphertext, const Key &key,
                      std::string *plaintext);

  virtual std::string name() const = 0;
  virtual Algorithms algorithm() const = 0;
  virtual unsigned key_size() const = 0;
  virtual unsigned iv_size() const = 0;
  virtual unsigned block_size() const = 0;

 protected:
  Cipher() { }
  virtual std::string DoEncrypt(const std::string &plaintext,
                                const Key &key) = 0;
  virtual std::string DoDecrypt(const std::string &ciphertext,
                                const Key &key) = 0;
};


/**
 * Uses OpenSSL EVP_... format.  The IV is created from the system time.
 */
class CVMFS_EXPORT CipherAes256Cbc : public Cipher {
  FRIEND_TEST(T_Encrypt, Aes_256_Cbc_Iv);

 public:
  static const unsigned kKeySize = 256 / 8;
  static const unsigned kIvSize = 128 / 8;
  static const unsigned kBlockSize = 128 / 8;

  virtual ~CipherAes256Cbc() { }

  virtual std::string name() const { return "AES-256-CBC"; }
  virtual Algorithms algorithm() const { return kAes256Cbc; }
  virtual unsigned key_size() const { return kKeySize; }
  virtual unsigned iv_size() const { return kIvSize; }
  virtual unsigned block_size() const { return kBlockSize; }

 protected:
  virtual std::string DoEncrypt(const std::string &plaintext, const Key &key);
  virtual std::string DoDecrypt(const std::string &ciphertext, const Key &key);

 private:
  shash::Md5 GenerateIv(const Key &key);
};


/**
 * No encryption, plaintext and ciphertext are identical.  For testing.
 */
class CVMFS_EXPORT CipherNone : public Cipher {
 public:
  virtual ~CipherNone() { }

  virtual std::string name() const { return "FOR TESTING ONLY"; }
  virtual Algorithms algorithm() const { return kNone; }
  virtual unsigned key_size() const { return 256 / 8; }
  virtual unsigned iv_size() const { return 128 / 8; }
  virtual unsigned block_size() const { return 128 / 8; }

 protected:
  virtual std::string DoEncrypt(const std::string &plaintext, const Key &key);
  virtual std::string DoDecrypt(const std::string &ciphertext, const Key &key);
};

}  // namespace cipher

#endif  // CVMFS_CRYPTO_ENCRYPT_H_
