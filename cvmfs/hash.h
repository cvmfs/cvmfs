/**
 * This file is part of the CernVM File System.
 *
 * Provides a bit syntactic sugar around the hash algorithms.
 * In particular, hashes can easily be created by constructors.
 * Also, we have a little to-string-from-string conversion.
 *
 * The complexity is due to the need to avoid dynamically allocated memory
 * for the hashes.  Almost everything happens on the stack.
 */

#ifndef CVMFS_HASH_H_
#define CVMFS_HASH_H_

#include <stdint.h>

#include <cstring>
#include <cassert>

#include <string>
#include "logging.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

namespace hash {

enum Algorithms {
  kMd5 = 0,
  kSha1,
  kAny,
};


/**
 * Corresponds to Algorithms.  "Any" is the maximum of all the other
 * digest sizes.
 */
const unsigned kDigestSizes[] = {16, 20, 20};
const unsigned kMaxDigestSize = 20;


/**
 * Distinguishes between interpreting a string as hex hash and hashing over
 * the contents of a string.
 */
struct HexPtr {
  const std::string *str;
  explicit HexPtr(const std::string &s) { str = &s; }
};

struct AsciiPtr {
  const std::string *str;
  explicit AsciiPtr(const std::string &s) { str = &s; }
};


/**
 * Holds a hash digest and provides from string / to string conversion and
 * comparison.  The kAny algorithm may not be used in functions!  The algorithm
 * has to be changed beforehand.
 * This class is not used directly, but used as base clase of Md5, Sha1, ...
 */
template<unsigned digest_size_, Algorithms algorithm_>
struct Digest {
  unsigned char digest[digest_size_];
  Algorithms algorithm;

  unsigned GetDigestSize() const { return kDigestSizes[algorithm]; }

  Digest() {
    algorithm = algorithm_;
    memset(digest, 0, digest_size_);
  }

  explicit Digest(const Algorithms a, const HexPtr hex) {
    algorithm = a;
    assert((algorithm_ == kAny) || (a == algorithm_));
    const unsigned char_size = 2*kDigestSizes[a];

    const std::string *str = hex.str;
    const unsigned length = str->length();
    assert(length >= char_size);

    for (unsigned i = 0; i < char_size; i += 2) {
      this->digest[i/2] =
        ((*str)[i] <= '9' ? (*str)[i] -'0' : (*str)[i] - 'a' + 10)*16 +
        ((*str)[i+1] <= '9' ? (*str)[i+1] - '0' : (*str)[i+1] - 'a' + 10);
    }
  }

  Digest(const Algorithms a,
         const unsigned char *digest_buffer, const unsigned buffer_size)
  {
    algorithm = a;
    assert(buffer_size <= digest_size_);
    memcpy(digest, digest_buffer, buffer_size);
  }

  void ToCStr(char cstr[digest_size_+1]) const {
    unsigned i;
    for (i = 0; i < kDigestSizes[algorithm]; ++i) {
      char dgt1 = (unsigned)digest[i] / 16;
      char dgt2 = (unsigned)digest[i] % 16;
      dgt1 += (dgt1 <= 9) ? '0' : 'a' - 10;
      dgt2 += (dgt2 <= 9) ? '0' : 'a' - 10;
      cstr[i*2] = dgt1;
      cstr[i*2+1] = dgt2;
    }
    cstr[i*2] = '\0';
  }

  std::string ToString() const {
    char result[2*kDigestSizes[algorithm]+1];
    ToCStr(result);
    return std::string(result, 2*kDigestSizes[algorithm]);
  }

  /**
   * Create a path string from the hex notation of the digest.
   */
  std::string MakePath(const unsigned dir_levels,
                       const unsigned digits_per_level) const
  {
    const unsigned string_length = 2*kDigestSizes[algorithm] + dir_levels + 1;
    std::string result(string_length, 0);

    unsigned i = 0, pos = 0;
    while (i < 2*kDigestSizes[algorithm]) {
      if (((i % digits_per_level) == 0) &&
          ((i / digits_per_level) <= dir_levels))
      {
        result[pos] = '/';
        ++pos;
      }
      char digit = ((i % 2) == 0) ? digest[i/2] / 16 : digest[i/2] % 16;
      digit += (digit <= 9) ? '0' : 'a' - 10;
      result[pos] = digit;
      ++pos;
      ++i;
    }

    return result;
  }

  bool IsNull() const {
    for (unsigned i = 0; i < kDigestSizes[algorithm]; ++i)
      if (digest[i] != 0)
        return false;
    return true;
  }

  bool operator ==(const Digest<digest_size_, algorithm_> &other) const {
    if (this->algorithm != other.algorithm)
      return false;
    for (unsigned i = 0; i < kDigestSizes[algorithm]; ++i)
      if (this->digest[i] != other.digest[i])
        return false;
    return true;
  }

  bool operator !=(const Digest<digest_size_, algorithm_> &other) const {
    return !(*this == other);
  }

  bool operator <(const Digest<digest_size_, algorithm_> &other) const {
    for (unsigned i = 0; i < kDigestSizes[algorithm]; ++i) {
      if (this->digest[i] > other.digest[i])
        return false;
      if (this->digest[i] < other.digest[i])
        return true;
    }
    return false;
  }

  bool operator >(const Digest<digest_size_, algorithm_> &other) const {
    for (int i = 0; i < kDigestSizes[algorithm]; ++i) {
      if (this->digest[i] < other.digest[i])
        return false;
      if (this->digest[i] > other.digest[i])
        return true;
    }
    return false;
  }
};


struct Md5 : public Digest<16, kMd5> {
  Md5() : Digest<16, kMd5>() { }
  explicit Md5(const AsciiPtr ascii);
  explicit Md5(const HexPtr hex) : Digest<16, kMd5>(kMd5, hex) { } ;
  Md5(const char *chars, const unsigned length);

  /**
   * An MD5 hash can be seen as two 64bit integers.
   */
  Md5(const uint64_t lo, const uint64_t hi);
  void ToIntPair(uint64_t *lo, uint64_t *hi) const;
};

struct Sha1 : public Digest<20, kSha1> { };

/**
 * Any as such must not be used except for digest storage.
 * To do real work, the class has to be "blessed" to be a real hash by
 * setting the algorithm field accordingly.
 */
struct Any : public Digest<20, kAny> {
  static Any randomHash(const Algorithms a);

  Any() : Digest<20, kAny>() { }
  explicit Any(const Algorithms a) : Digest<20, kAny>() { algorithm = a; }
  Any(const Algorithms a,
      const unsigned char *digest_buffer, const unsigned buffer_size)
    : Digest<20, kAny>(a, digest_buffer, buffer_size) { }
  explicit Any(const Algorithms a, const HexPtr hex) :
    Digest<20, kAny>(a, hex) { }
};


/**
 * Actual operations on digests, like "hash a file", "hash a buffer", or
 * iterative operations.
 */
unsigned GetContextSize(const Algorithms algorithm);

/**
 * Holds an OpenSSL context, only required for hash operations.  Allows to
 * deferr the storage allocation for the context to alloca.
 */
struct ContextPtr {
  Algorithms algorithm;
  void *buffer;
  unsigned size;

  ContextPtr() {
    algorithm = kAny;
    size = 0;
    buffer = NULL;
  }

  explicit ContextPtr(const Algorithms a) {
    algorithm = a;
    size = GetContextSize(a);
    buffer = NULL;
  }
};

void Init(ContextPtr context);
void Update(const unsigned char *buffer, const unsigned buffer_size,
            ContextPtr context);
void Final(ContextPtr context, Any *any_digest);
void HashMem(const unsigned char *buffer, const unsigned buffer_size,
             Any *any_digest);
bool HashFile(const std::string filename, Any *any_digest);

}  // namespace hash

#ifdef CVMFS_NAMESPACE_GUARD
}
#endif

#endif  // CVMFS_HASH_H_
