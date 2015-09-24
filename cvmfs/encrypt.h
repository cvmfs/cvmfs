/**
 * This file is part of the CernVM File System
 */

#ifndef CVMFS_ENCRYPT_H_
#define CVMFS_ENCRYPT_H_

#include <string>

#include "util.h"

namespace cipher {

enum Algorithms {
  kAes256Cbc = 0,
  kNone,
};


/**
 * Encapsulates a small non-copyable piece of pinned memory.  Is actively set to
 * zero on destruction.
 */
class Key : SingleCopy {
 public:
  static const unsigned kMaxSize = 64;

  static Key *CreateRandomly(const unsigned size);
  static Key *CreateFromFile(const std::string &path);
  bool SaveToFile(const std::string &path);
  ~Key();

  unsigned size() const { return size_; }
  const unsigned char *data() const { return data_; }

 private:
  Key() : data_(NULL), size_(0)  { }
  unsigned char *data_;
  unsigned size_;
};


/**
 * The interface for an encryption algorithm.  Uses a simple envelope with a one
 * byte prefix: the first 4 bits are the envelope version (currently 0), the
 * second 4 bits are the encryption algorithm and refers to Algorithms.
 */
class Cipher {
 public:
  static Cipher *Create(const Algorithms a);

  bool Encrypt(const std::string &plaintext, const Key &key,
               std::string *ciphertext);
  static bool Decrypt(const std::string &ciphertext, const Key &key,
                      std::string *plaintext);

  virtual std::string const name() = 0;
  virtual Algorithms const algorithm() = 0;
  virtual unsigned const key_size() = 0;
  virtual unsigned const iv_size() = 0;
  virtual unsigned const block_size() = 0;

 protected:
  Cipher() { }
  virtual std::string DoEncrypt(const std::string &plaintext,
                                const Key &key) = 0;
  virtual std::string DoDecrypt(const std::string &ciphertext,
                                const Key &key) = 0;
};


/**
 * Uses openssl EVP_... format.  The IV is created from the system time.
 */
class CipherAes256Cbc : public Cipher {
 public:
  virtual std::string const name() { return "AES-256-CBC"; }
  virtual Algorithms const algorithm() { return kAes256Cbc; }
  virtual unsigned const key_size() { return 256/8; }
  virtual unsigned const iv_size() { return 128/8; }
  virtual unsigned const block_size() { return 128/8; }

 protected:
  virtual std::string DoEncrypt(const std::string &plaintext, const Key &key);
  virtual std::string DoDecrypt(const std::string &ciphertext, const Key &key);
};


/**
 * No encryption, plaintext and ciphertext are identical.  For testing.
 */
class CipherNone : public Cipher {
 public:
  virtual std::string const name() { return "FOR TESTING ONLY"; }
  virtual Algorithms const algorithm() { return kNone; }
  virtual unsigned const key_size() { return 256/8; }
  virtual unsigned const iv_size() { return 128/8; }
  virtual unsigned const block_size() { return 128/8; }

 protected:
  virtual std::string DoEncrypt(const std::string &plaintext, const Key &key);
  virtual std::string DoDecrypt(const std::string &ciphertext, const Key &key);
};

}  // namespace cipher

#endif  // CVMFS_ENCRYPT_H_
