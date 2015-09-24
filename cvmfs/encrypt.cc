/**
 * This file is part of the CernVM File System
 */

#include "cvmfs_config.h"
#include "encrypt.h"

#include <fcntl.h>
#include <openssl/rand.h>

#include <cassert>
#include <cstdlib>
#include <cstring>

#include "platform.h"
#include "smalloc.h"
#include "util.h"

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
  if (info.st_size > kMaxSize) {
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



string CipherAes256Cbc::DoEncrypt(const string &plaintext, const Key &key) {
  return "";
}


string CipherAes256Cbc::DoDecrypt(const string &ciphertext, const Key &key) {
  return "";
}


//------------------------------------------------------------------------------


string CipherNone::DoEncrypt(const string &plaintext, const Key &key) {
  return plaintext;
}


string CipherNone::DoDecrypt(const string &ciphertext, const Key &key) {
  return ciphertext;
}

}  // namespace cipher
