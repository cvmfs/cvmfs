/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "hash.h"

#include <alloca.h>
#include <openssl/md5.h>
#include <openssl/sha.h>
#include <openssl/ripemd.h>

#include <cstdio>

using namespace std;  // NOLINT

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

namespace shash {

const char *kSuffixes[] = {"", "", "-rmd160", ""};

Algorithms ParseHashAlgorithm(const string &algorithm_option) {
  if (algorithm_option == "sha1")
    return kSha1;
  if (algorithm_option == "rmd160")
    return kRmd160;
  return kAny;
}


Algorithms HexPtr::DetermineAlgorithm() const {
  static const Algorithms algorithms[] = { kMd5, kSha1, kRmd160 };
  static const size_t num_algos  = sizeof(algorithms) / sizeof(algorithms[0]);

  // TODO: check hash suffixes!
  const size_t len = str->length();
  for (unsigned int i = 0; i < num_algos; ++i) {
    const Algorithms algorithm = algorithms[i];
    if (len == 2*kDigestSizes[algorithm] + kSuffixLengths[algorithm]) {
      return algorithm;
    }
  }

  return kUnknown;
}


Any MkFromHexPtr(const HexPtr hex) {
  Any result;

  const unsigned length = hex.str->length();
  if (length == 2*kDigestSizes[kMd5])
    result = Any(kMd5, hex);
  if (length == 2*kDigestSizes[kSha1])
    result = Any(kSha1, hex);
  // TODO compare -rmd160
  if ((length == 2*kDigestSizes[kRmd160] + kSuffixLengths[kRmd160]))
    result = Any(kRmd160, hex);

  return result;
}


/**
 * Allows the caller to create the context on the stack.
 */
unsigned GetContextSize(const Algorithms algorithm) {
  switch (algorithm) {
    case kMd5:
      return sizeof(MD5_CTX);
    case kSha1:
      return sizeof(SHA_CTX);
    case kRmd160:
      return sizeof(RIPEMD160_CTX);
    default:
      LogCvmfs(kLogHash, kLogDebug | kLogSyslogErr, "tried to generate hash "
               "context for unspecified hash. Aborting...");
      abort();  // Undefined hash
  }
}

void Init(ContextPtr &context) {
  switch (context.algorithm) {
    case kMd5:
      assert(context.size == sizeof(MD5_CTX));
      MD5_Init(reinterpret_cast<MD5_CTX *>(context.buffer));
      break;
    case kSha1:
      assert(context.size == sizeof(SHA_CTX));
      SHA1_Init(reinterpret_cast<SHA_CTX *>(context.buffer));
      break;
    case kRmd160:
      assert(context.size == sizeof(RIPEMD160_CTX));
      RIPEMD160_Init(reinterpret_cast<RIPEMD160_CTX *>(context.buffer));
      break;
    default:
      abort();  // Undefined hash
  }
}

void Update(const unsigned char *buffer, const unsigned buffer_length,
            ContextPtr &context)
{
  switch (context.algorithm) {
    case kMd5:
      assert(context.size == sizeof(MD5_CTX));
      MD5_Update(reinterpret_cast<MD5_CTX *>(context.buffer),
                 buffer, buffer_length);
      break;
    case kSha1:
      assert(context.size == sizeof(SHA_CTX));
      SHA1_Update(reinterpret_cast<SHA_CTX *>(context.buffer),
                  buffer, buffer_length);
      break;
    case kRmd160:
      assert(context.size == sizeof(RIPEMD160_CTX));
      RIPEMD160_Update(reinterpret_cast<RIPEMD160_CTX *>(context.buffer),
                       buffer, buffer_length);
      break;
    default:
      abort();  // Undefined hash
  }
}

void Final(ContextPtr &context, Any *any_digest) {
  switch (context.algorithm) {
    case kMd5:
      assert(context.size == sizeof(MD5_CTX));
      MD5_Final(any_digest->digest,
                reinterpret_cast<MD5_CTX *>(context.buffer));
      break;
    case kSha1:
      assert(context.size == sizeof(SHA_CTX));
      SHA1_Final(any_digest->digest,
                 reinterpret_cast<SHA_CTX *>(context.buffer));
      break;
    case kRmd160:
      assert(context.size == sizeof(RIPEMD160_CTX));
      RIPEMD160_Final(any_digest->digest,
                      reinterpret_cast<RIPEMD160_CTX *>(context.buffer));
      break;
    default:
      abort();  // Undefined hash
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


bool HashFile(const std::string filename, Any *any_digest) {
  FILE *file = fopen(filename.c_str(), "r");
  if (file == NULL)
    return false;

  Algorithms algorithm = any_digest->algorithm;
  ContextPtr context(algorithm);
  context.buffer = alloca(context.size);

  Init(context);
  unsigned char io_buffer[4096];
  int actual_bytes;
  while ((actual_bytes = fread(io_buffer, 1, 4096, file))) {
    Update(io_buffer, actual_bytes, context);
  }

  if (ferror(file)) {
    fclose(file);
    return false;
  }

  Final(context, any_digest);
  fclose(file);
  return true;
}


/**
 * Fast constructor for hashing path names.
 */
Md5::Md5(const AsciiPtr ascii) {
  algorithm = kMd5;
  const string *str = ascii.str;

  MD5_CTX md5_state;
  MD5_Init(&md5_state);
  MD5_Update(&md5_state, reinterpret_cast<const unsigned char *>(&(*str)[0]),
             str->length());
  MD5_Final(digest, &md5_state);
}


Md5::Md5(const char *chars, const unsigned length) {
  algorithm = kMd5;

  MD5_CTX md5_state;
  MD5_Init(&md5_state);
  MD5_Update(&md5_state, reinterpret_cast<const unsigned char *>(chars),
             length);
  MD5_Final(digest, &md5_state);
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

}  // namespace hash

#ifdef CVMFS_NAMESPACE_GUARD
}
#endif
