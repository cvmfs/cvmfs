/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "crypto/hash.h"

#include <alloca.h>
#include <errno.h>
#include <fcntl.h>
#include <nettle/md5.h>
#include <nettle/ripemd160.h>
#include <nettle/sha1.h>
#include <nettle/sha2.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>

#include "util/exception.h"
#include "KeccakHash.h"


using namespace std;  // NOLINT

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

namespace shash {

const char *kAlgorithmIds[] =
  {"", "", "-rmd160", "-shake128", ""};


bool HexPtr::IsValid() const {
  const unsigned l = str->length();
  if (l == 0)
    return false;
  const char *c = str->data();  // Walks through the string
  unsigned i = 0;  // String position of *c

  for ( ; i < l; ++i, ++c) {
    if (*c == '-')
      break;
    if ((*c < '0') || (*c > 'f') || ((*c > '9') && (*c < 'a')))
      return false;
  }

  // Walk through all algorithms
  for (unsigned j = 0; j < kAny; ++j) {
    const unsigned hex_length = 2*kDigestSizes[j];
    const unsigned algo_id_length = kAlgorithmIdSizes[j];
    if (i == hex_length) {
      // Right suffix?
      for ( ; (i < l) && (i-hex_length < algo_id_length); ++i, ++c) {
        if (*c != kAlgorithmIds[j][i-hex_length])
          break;
      }
      if ((i == l) && (l == hex_length + algo_id_length))
        return true;
      i = hex_length;
      c = str->data() + i;
    }
  }

  return false;
}


Algorithms ParseHashAlgorithm(const string &algorithm_option) {
  if (algorithm_option == "sha1")
    return kSha1;
  if (algorithm_option == "rmd160")
    return kRmd160;
  if (algorithm_option == "shake128")
    return kShake128;
  return kAny;
}


Any MkFromHexPtr(const HexPtr hex, const char suffix) {
  Any result;

  const unsigned length = hex.str->length();
  if (length == 2*kDigestSizes[kMd5])
    result = Any(kMd5, hex);
  if (length == 2*kDigestSizes[kSha1])
    result = Any(kSha1, hex);
  // TODO(jblomer) compare -rmd160, -shake128
  if ((length == 2*kDigestSizes[kRmd160] + kAlgorithmIdSizes[kRmd160]))
    result = Any(kRmd160, hex);
  if ((length == 2*kDigestSizes[kShake128] + kAlgorithmIdSizes[kShake128]))
    result = Any(kShake128, hex);

  result.suffix = suffix;
  return result;
}


/**
 * Similar to MkFromHexPtr but the suffix is deducted from the HexPtr string.
 */
Any MkFromSuffixedHexPtr(const HexPtr hex) {
  Any result;

  const unsigned length = hex.str->length();
  if ((length == 2*kDigestSizes[kMd5]) || (length == 2*kDigestSizes[kMd5] + 1))
  {
    Suffix suffix = (length == 2*kDigestSizes[kMd5] + 1) ?
      *(hex.str->rbegin()) : kSuffixNone;
    result = Any(kMd5, hex, suffix);
  }
  if ((length == 2*kDigestSizes[kSha1]) ||
      (length == 2*kDigestSizes[kSha1] + 1))
  {
    Suffix suffix = (length == 2*kDigestSizes[kSha1] + 1) ?
      *(hex.str->rbegin()) : kSuffixNone;
    result = Any(kSha1, hex, suffix);
  }
  if ((length == 2*kDigestSizes[kRmd160] + kAlgorithmIdSizes[kRmd160]) ||
      (length == 2*kDigestSizes[kRmd160] + kAlgorithmIdSizes[kRmd160] + 1))
  {
    Suffix suffix =
      (length == 2*kDigestSizes[kRmd160] + kAlgorithmIdSizes[kRmd160] + 1)
        ? *(hex.str->rbegin())
        : kSuffixNone;
    result = Any(kRmd160, hex, suffix);
  }
  if ((length == 2*kDigestSizes[kShake128] + kAlgorithmIdSizes[kShake128]) ||
      (length == 2*kDigestSizes[kShake128] + kAlgorithmIdSizes[kShake128] + 1))
  {
    Suffix suffix =
      (length == 2*kDigestSizes[kShake128] + kAlgorithmIdSizes[kShake128] + 1)
        ? *(hex.str->rbegin())
        : kSuffixNone;
    result = Any(kShake128, hex, suffix);
  }

  return result;
}


/**
 * Allows the caller to create the context on the stack.
 */
unsigned GetContextSize(const Algorithms algorithm) {
  switch (algorithm) {
    case kMd5:
      return sizeof(md5_ctx);
    case kSha1:
      return sizeof(sha1_ctx);
    case kRmd160:
      return sizeof(ripemd160_ctx);
    case kShake128:
      return sizeof(Keccak_HashInstance);
    default:
      PANIC(kLogDebug | kLogSyslogErr,
            "tried to generate hash context for unspecified hash. Aborting...");
  }
}

void Init(ContextPtr context) {
  HashReturn keccak_result;
  switch (context.algorithm) {
    case kMd5:
      md5_init(reinterpret_cast<md5_ctx *>(context.buffer));
      break;
    case kSha1:
      sha1_init(reinterpret_cast<sha1_ctx *>(context.buffer));
      break;
    case kRmd160:
      ripemd160_init(reinterpret_cast<ripemd160_ctx *>(context.buffer));
      break;
    case kShake128:
      keccak_result = Keccak_HashInitialize_SHAKE128(
        reinterpret_cast<Keccak_HashInstance *>(context.buffer));
      assert(keccak_result == SUCCESS);
      break;
    default:
      PANIC(NULL);  // Undefined hash
  }
}

void Update(const unsigned char *buffer, const unsigned buffer_length,
            ContextPtr context)
{
  HashReturn keccak_result;
  switch (context.algorithm) {
    case kMd5:
      md5_update(reinterpret_cast<md5_ctx *>(context.buffer),
                  buffer_length, buffer);
      break;
    case kSha1:
      sha1_update(reinterpret_cast<sha1_ctx *>(context.buffer),
                  buffer_length, buffer);
      break;
    case kRmd160:
      ripemd160_update(reinterpret_cast<ripemd160_ctx *>(context.buffer),
                       buffer_length, buffer);
      break;
    case kShake128:
      assert(context.size == sizeof(Keccak_HashInstance));
      keccak_result = Keccak_HashUpdate(reinterpret_cast<Keccak_HashInstance *>(
                        context.buffer), buffer, buffer_length * 8);
      assert(keccak_result == SUCCESS);
      break;
    default:
      PANIC(NULL);  // Undefined hash
  }
}

void Final(ContextPtr context, Any *any_digest) {
  HashReturn keccak_result;
  switch (context.algorithm) {
    case kMd5:
      md5_digest(reinterpret_cast<md5_ctx *>(context.buffer),
                 MD5_DIGEST_SIZE, any_digest->digest);
      break;
    case kSha1:
      sha1_digest(reinterpret_cast<sha1_ctx *>(context.buffer),
                  SHA1_DIGEST_SIZE, any_digest->digest);
      break;
    case kRmd160:
      ripemd160_digest(reinterpret_cast<ripemd160_ctx *>(context.buffer),
                       RIPEMD160_DIGEST_SIZE, any_digest->digest);
      break;
    case kShake128:
      keccak_result = Keccak_HashFinal(reinterpret_cast<Keccak_HashInstance *>(
                        context.buffer), NULL);
      assert(keccak_result == SUCCESS);
      keccak_result =
        Keccak_HashSqueeze(reinterpret_cast<Keccak_HashInstance *>(
          context.buffer), any_digest->digest, kDigestSizes[kShake128] * 8);
      break;
    default:
      PANIC(NULL);  // Undefined hash
  }
  any_digest->algorithm = context.algorithm;
}


void HashMem(const unsigned char *buffer, const unsigned buffer_size,
             Any *any_digest)
{
  Algorithms algorithm = any_digest->algorithm;
  ContextPtr context(algorithm);
  context.buffer = alloca(context.size);

  Init(context);
  Update(buffer, buffer_size, context);
  Final(context, any_digest);
}


void HashString(const std::string &content, Any *any_digest) {
  HashMem(reinterpret_cast<const unsigned char *>(content.data()),
          content.length(), any_digest);
}


void Hmac(
  const string &key,
  const unsigned char *buffer,
  const unsigned buffer_size,
  Any *any_digest
) {
  Algorithms algorithm = any_digest->algorithm;
  assert(algorithm != kAny);

  const unsigned block_size = kBlockSizes[algorithm];
  unsigned char key_block[block_size];
  memset(key_block, 0, block_size);
  if (key.length() > block_size) {
    Any hash_key(algorithm);
    HashMem(reinterpret_cast<const unsigned char *>(key.data()),
            key.length(), &hash_key);
    memcpy(key_block, hash_key.digest, kDigestSizes[algorithm]);
  } else {
    if (key.length() > 0)
      memcpy(key_block, key.data(), key.length());
  }

  unsigned char pad_block[block_size];
  // Inner hash
  Any hash_inner(algorithm);
  ContextPtr context_inner(algorithm);
  context_inner.buffer = alloca(context_inner.size);
  Init(context_inner);
  for (unsigned i = 0; i < block_size; ++i)
    pad_block[i] = key_block[i] ^ 0x36;
  Update(pad_block, block_size, context_inner);
  Update(buffer, buffer_size, context_inner);
  Final(context_inner, &hash_inner);

  // Outer hash
  ContextPtr context_outer(algorithm);
  context_outer.buffer = alloca(context_outer.size);
  Init(context_outer);
  for (unsigned i = 0; i < block_size; ++i)
    pad_block[i] = key_block[i] ^ 0x5c;
  Update(pad_block, block_size, context_outer);
  Update(hash_inner.digest, kDigestSizes[algorithm], context_outer);

  Final(context_outer, any_digest);
}


bool HashFd(int fd, Any *any_digest) {
  Algorithms algorithm = any_digest->algorithm;
  ContextPtr context(algorithm);
  context.buffer = alloca(context.size);

  Init(context);
  unsigned char io_buffer[4096];
  int actual_bytes;
  while ((actual_bytes = read(fd, io_buffer, 4096)) != 0) {
    if (actual_bytes == -1) {
      if (errno == EINTR)
        continue;
      return false;
    }
    Update(io_buffer, actual_bytes, context);
  }
  Final(context, any_digest);
  return true;
}


bool HashFile(const std::string &filename, Any *any_digest) {
  int fd = open(filename.c_str(), O_RDONLY);
  if (fd == -1)
    return false;

  bool result = HashFd(fd, any_digest);
  close(fd);
  return result;
}


/**
 * Fast constructor for hashing path names.
 */
Md5::Md5(const AsciiPtr ascii) {
  algorithm = kMd5;
  const string *str = ascii.str;

  md5_ctx md5_state;
  md5_init(&md5_state);
  md5_update(&md5_state, str->length(),
             reinterpret_cast<const uint8_t *>(&(*str)[0]));
  md5_digest(&md5_state, MD5_DIGEST_SIZE, digest);
}


Md5::Md5(const char *chars, const unsigned length) {
  algorithm = kMd5;

  md5_ctx md5_state;
  md5_init(&md5_state);
  md5_update(&md5_state, length, reinterpret_cast<const uint8_t *>(chars));
  md5_digest(&md5_state, MD5_DIGEST_SIZE, digest);
}


Md5::Md5(const uint64_t lo, const uint64_t hi) {
  algorithm = kMd5;
  memcpy(digest, &lo, 8);
  memcpy(digest+8, &hi, 8);
}

void Md5::ToIntPair(uint64_t *lo, uint64_t *hi) const {
  memcpy(lo, digest, 8);
  memcpy(hi, digest+8, 8);
}


Md5 Any::CastToMd5() {
  assert(algorithm == kMd5);
  Md5 result;
  memcpy(result.digest, digest, kDigestSizes[kMd5]);
  return result;
}

static string HexFromSha256(unsigned char digest[SHA256_DIGEST_SIZE]) {
  string result;
  result.reserve(2 * SHA256_DIGEST_SIZE);
  for (unsigned i = 0; i < SHA256_DIGEST_SIZE; ++i) {
    const char d1 = digest[i] / 16;
    const char d2 = digest[i] % 16;
    result.push_back(d1 + ((d1 <= 9) ? '0' : 'a' - 10));
    result.push_back(d2 + ((d2 <= 9) ? '0' : 'a' - 10));
  }
  return result;
}

string Sha256File(const string &filename) {
  int fd = open(filename.c_str(), O_RDONLY);
  if (fd < 0)
    return "";

  sha256_ctx ctx;
  sha256_init(&ctx);

  unsigned char io_buffer[4096];
  int actual_bytes;
  while ((actual_bytes = read(fd, io_buffer, 4096)) != 0) {
    if (actual_bytes == -1) {
      if (errno == EINTR)
        continue;
      close(fd);
      return "";
    }
    sha256_update(&ctx, actual_bytes, io_buffer);
  }
  close(fd);

  unsigned char digest[SHA256_DIGEST_SIZE];
  sha256_digest(&ctx, SHA256_DIGEST_SIZE, digest);
  return HexFromSha256(digest);
}

string Sha256Mem(const unsigned char *buffer, const unsigned buffer_size) {
  unsigned char digest[SHA256_DIGEST_SIZE];
  sha256_ctx ctx;
  sha256_init(&ctx);
  sha256_update(&ctx, buffer_size, buffer);
  sha256_digest(&ctx, SHA256_DIGEST_SIZE, digest);
  return HexFromSha256(digest);
}

string Sha256String(const string &content) {
  return Sha256Mem(reinterpret_cast<const unsigned char *>(content.data()),
                   content.length());
}


std::string Hmac256(
  const std::string &key,
  const std::string &content,
  bool raw_output)
{
  sha256_ctx ctx;
  unsigned char digest[SHA256_DIGEST_SIZE];
  const unsigned block_size = 64;
  const unsigned key_length = key.length();
  unsigned char key_block[block_size];
  memset(key_block, 0, block_size);
  if (key_length > block_size) {
    sha256_init(&ctx);
    sha256_update(&ctx, key_length,
                  reinterpret_cast<const unsigned char *>(key.data()));
    sha256_digest(&ctx, SHA256_DIGEST_SIZE, key_block);
  } else {
    if (key.length() > 0)
      memcpy(key_block, key.data(), key_length);
  }

  unsigned char pad_block[block_size];
  // Inner hash
  unsigned char digest_inner[SHA256_DIGEST_SIZE];
  sha256_init(&ctx);
  for (unsigned i = 0; i < block_size; ++i)
    pad_block[i] = key_block[i] ^ 0x36;
  sha256_update(&ctx, block_size, pad_block);
  sha256_update(&ctx, content.length(),
                reinterpret_cast<const unsigned char *>(content.data()));
  sha256_digest(&ctx, SHA256_DIGEST_SIZE, digest_inner);

  // Outer hash
  sha256_init(&ctx);
  for (unsigned i = 0; i < block_size; ++i)
    pad_block[i] = key_block[i] ^ 0x5c;
  sha256_update(&ctx, block_size, pad_block);
  sha256_update(&ctx, SHA256_DIGEST_SIZE, digest_inner);

  sha256_digest(&ctx, SHA256_DIGEST_SIZE, digest);
  if (raw_output)
    return string(reinterpret_cast<const char *>(digest), SHA256_DIGEST_SIZE);
  return HexFromSha256(digest);
}

}  // namespace shash

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif
