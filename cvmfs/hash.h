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

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <string>

#include "logging.h"
#include "prng.h"
#include "smalloc.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

namespace shash {

/**
 * Don't change order!  The integer value of the enum constants is used
 * as file catalog flags and as flags in communication with the cache manager.
 */
enum Algorithms {
  kMd5 = 0,
  kSha1,
  kRmd160,
  kAny,
};

const char kSuffixNone         = 0;
const char kSuffixCatalog      = 'C';
const char kSuffixHistory      = 'H';
const char kSuffixMicroCatalog = 'L';  // currently unused
const char kSuffixPartial      = 'P';
const char kSuffixTemporary    = 'T';
const char kSuffixCertificate  = 'X';


/**
 * Corresponds to Algorithms.  "Any" is the maximum of all the other
 * digest sizes.
 */
const unsigned kDigestSizes[] = {16, 20, 20, 20};
const unsigned kMaxDigestSize = 20;
/**
 * Hex representations of hashes with the same length need a suffix
 * to be distinguished from each other.  They should all have one but
 * for backwards compatibility MD5 and SHA-1 have none.
 */
extern const char *kAlgorithmIds[];
// in hash.cc: const char *kAlgorithmIds[] = {"", "", "-rmd160", ""};
const unsigned kAlgorithmIdSizes[] = {0, 0, 7, 0};
const unsigned kMaxAlgorithmIdentifierSize = 7;

/**
 * Corresponds to Algorithms.  There is no block size for Any
 */
const unsigned kBlockSizes[] = {64, 64, 64};


/**
 * Distinguishes between interpreting a string as hex hash and hashing over
 * the contents of a string.
 */
struct HexPtr {
  const std::string *str;
  explicit HexPtr(const std::string &s) { str = &s; }
  bool IsValid() const;
};

struct AsciiPtr {
  const std::string *str;
  explicit AsciiPtr(const std::string &s) { str = &s; }
};

typedef char Suffix;

/**
 * Holds a hash digest and provides from string / to string conversion and
 * comparison.  The kAny algorithm may not be used in functions!  The algorithm
 * has to be changed beforehand.
 * This class is not used directly, but used as base clase of Md5, Sha1, ...
 */
template<unsigned digest_size_, Algorithms algorithm_>
struct Digest {
  unsigned char digest[digest_size_];
  Algorithms    algorithm;
  Suffix        suffix;

  class Hex {
   public:
    explicit Hex(const Digest<digest_size_, algorithm_> *digest) :
      digest_(*digest),
      hash_length_(2 * kDigestSizes[digest_.algorithm]),
      algo_id_length_(kAlgorithmIdSizes[digest_.algorithm]) {}

    unsigned int length() const { return hash_length_ + algo_id_length_; }

    char operator[](const unsigned int position) const {
      assert(position < length());
      return (position < hash_length_)
        ? GetHashChar(position)
        : GetAlgorithmIdentifierChar(position);
    }

   protected:
    char GetHashChar(const unsigned int position) const {
      assert(position < hash_length_);
      const char digit = (position % 2 == 0)
        ? digest_.digest[position / 2] / 16
        : digest_.digest[position / 2] % 16;
      return ToHex(digit);
    }

    char GetAlgorithmIdentifierChar(const unsigned int position) const {
      assert(position >= hash_length_);
      return kAlgorithmIds[digest_.algorithm][position - hash_length_];
    }

    char ToHex(const char c) const { return c + ((c <= 9) ? '0' : 'a' - 10); }

   private:
    const Digest<digest_size_, algorithm_>  &digest_;
    const unsigned int                       hash_length_;
    const unsigned int                       algo_id_length_;
  };

  unsigned GetDigestSize() const { return kDigestSizes[algorithm]; }
  unsigned GetHexSize() const {
    return 2*kDigestSizes[algorithm] + kAlgorithmIdSizes[algorithm];
  }

  Digest() :
    algorithm(algorithm_), suffix(kSuffixNone)
  {
    memset(digest, 0, digest_size_);
  }

  explicit Digest(const Algorithms a, const HexPtr hex, const char s = 0) :
    algorithm(a), suffix(s)
  {
    assert((algorithm_ == kAny) || (a == algorithm_));
    const unsigned char_size = 2*kDigestSizes[a];

    const std::string *str = hex.str;
    const unsigned length = str->length();
    assert(length >= char_size);  // A suffix won't hurt

    for (unsigned i = 0; i < char_size; i += 2) {
      this->digest[i/2] =
        ((*str)[i] <= '9' ? (*str)[i] -'0' : (*str)[i] - 'a' + 10)*16 +
        ((*str)[i+1] <= '9' ? (*str)[i+1] - '0' : (*str)[i+1] - 'a' + 10);
    }
  }

  Digest(const Algorithms a,
         const unsigned char *digest_buffer, const unsigned buffer_size,
         const Suffix s = kSuffixNone) :
    algorithm(a), suffix(s)
  {
    assert(buffer_size <= digest_size_);
    memcpy(digest, digest_buffer, buffer_size);
  }

  /**
   * Generates a purely random hash
   * Only used for testing purposes
   */
  void Randomize() {
    Prng prng;
    prng.InitLocaltime();
    Randomize(&prng);
  }

  /**
   * Generates a purely random hash
   * Only used for testing purposes
   *
   * @param seed  random number generator seed (for reproducability)
   */
  void Randomize(const uint64_t seed) {
    Prng prng;
    prng.InitSeed(seed);
    Randomize(&prng);
  }

  /**
   * Generates a purely random hash
   * Only used for testing purposes
   *
   * @param prng  random number generator object (for external reproducability)
   */
  void Randomize(Prng *prng) {
    const unsigned bytes = GetDigestSize();
    for (unsigned i = 0; i < bytes; ++i) {
      digest[i] = prng->Next(256);
    }
  }

  bool HasSuffix() const { return suffix != kSuffixNone; }
  void set_suffix(const Suffix s) { suffix = s; }

  std::string ToString(const bool with_suffix = false) const {
    Hex hex(this);
    const bool     use_suffix  = with_suffix && HasSuffix();
    const unsigned string_length = hex.length() + use_suffix;
    std::string result(string_length, 0);

    for (unsigned int i = 0; i < hex.length(); ++i) {
      result[i] = hex[i];
    }

    if (use_suffix) {
      result[string_length - 1] = suffix;
    }

    assert(result.length() == string_length);
    return result;
  }

  std::string ToStringWithSuffix() const {
    return ToString(true);
  }

  std::string MakePath(const std::string &prefix = "data") const {
    return MakePathExplicit(1, 2, prefix, shash::kSuffixNone);
  }

  /**
   * Note: This is a crutch method until we replace MakePathExplicit() anywhere
   *       with MakePath() which will handle this suffix magic internally.
   */
  std::string MakePathWithSuffix(const unsigned   dir_levels,
                                 const unsigned   digits_per_level,
                                 const Suffix     hash_suffix) const {
    const std::string no_prefix = "";
    return MakePathExplicit(dir_levels,
                            digits_per_level,
                            no_prefix,
                            hash_suffix);
  }

  std::string MakePathWithSuffix(const std::string &prefix = "data") const {
    return MakePathExplicit(1, 2, prefix, suffix);
  }

  /**
   * Create a path string from the hex notation of the digest.
   * Note: This method takes an explicit suffix. This is a crutch to allow for
   *       MakePathWithSuffix() until MakePathExplicit() is replaced by the more
   *       convenient MakePath() everywhere in the code. Then this method will
   *       use the member variable suffix by default.
   */
  std::string MakePathExplicit(
                          const unsigned      dir_levels,
                          const unsigned      digits_per_level,
                          const std::string  &prefix = "",
                          const Suffix        hash_suffix = kSuffixNone) const {
    Hex hex(this);
    const bool use_suffix = (hash_suffix != kSuffixNone);

    const unsigned string_length =   prefix.length()
                                   + hex.length()
                                   + dir_levels
                                   + 1  // slash between prefix and hash
                                   + use_suffix;
    // prepend prefix string
    std::string result(prefix);
    result.resize(string_length);

    // build hexified hash and path delimiters
    unsigned pos = prefix.length();
    for (unsigned i = 0; i < hex.length(); ++i) {
      if ((i % digits_per_level == 0) && (i / digits_per_level <= dir_levels)) {
        result[pos++] = '/';
      }
      result[pos++] = hex[i];
    }

    // (optionally) add hash hint suffix
    if (use_suffix) {
      result[pos++] = hash_suffix;
    }

    assert(pos == string_length);
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
    if (this->algorithm != other.algorithm)
      return (this->algorithm < other.algorithm);
    for (unsigned i = 0; i < kDigestSizes[algorithm]; ++i) {
      if (this->digest[i] > other.digest[i])
        return false;
      if (this->digest[i] < other.digest[i])
        return true;
    }
    return false;
  }

  bool operator >(const Digest<digest_size_, algorithm_> &other) const {
    if (this->algorithm != other.algorithm)
      return (this->algorithm > other.algorithm);
    for (unsigned i = 0; i < kDigestSizes[algorithm]; ++i) {
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
  explicit Md5(const HexPtr hex) : Digest<16, kMd5>(kMd5, hex) { }
  Md5(const char *chars, const unsigned length);

  /**
   * An MD5 hash can be seen as two 64bit integers.
   */
  Md5(const uint64_t lo, const uint64_t hi);
  void ToIntPair(uint64_t *lo, uint64_t *hi) const;
};

struct Sha1 : public Digest<20, kSha1> { };
struct Rmd160 : public Digest<20, kRmd160> { };

/**
 * Any as such must not be used except for digest storage.
 * To do real work, the class has to be "blessed" to be a real hash by
 * setting the algorithm field accordingly.
 */
struct Any : public Digest<20, kAny> {
  Any() : Digest<20, kAny>() { }

  explicit Any(const Algorithms a,
               const char       s = kSuffixNone) :
    Digest<20, kAny>() { algorithm = a; suffix = s; }

  Any(const Algorithms     a,
      const unsigned char *digest_buffer, const unsigned buffer_size,
      const Suffix         suffix = kSuffixNone) :
    Digest<20, kAny>(a, digest_buffer, buffer_size, suffix) { }

  explicit Any(const Algorithms  a,
               const HexPtr      hex,
               const char        suffix = kSuffixNone) :
    Digest<20, kAny>(a, hex, suffix) { }
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
class ContextPtr {
 public:
  Algorithms  algorithm;
  void       *buffer;
  unsigned    size;

  ContextPtr() : algorithm(kAny), buffer(NULL), size(0) {}

  explicit ContextPtr(const Algorithms a) :
    algorithm(a), buffer(NULL), size(GetContextSize(a)) {}
};

void Init(ContextPtr context);
void Update(const unsigned char *buffer, const unsigned buffer_size,
            ContextPtr context);
void Final(ContextPtr context, Any *any_digest);
void HashMem(const unsigned char *buffer, const unsigned buffer_size,
             Any *any_digest);
void Hmac(const std::string &key,
          const unsigned char *buffer, const unsigned buffer_size,
          Any *any_digest);
bool HashFile(const std::string filename, Any *any_digest);

Algorithms ParseHashAlgorithm(const std::string &algorithm_option);
Any MkFromHexPtr(const HexPtr hex, const Suffix suffix = kSuffixNone);

}  // namespace shash

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_HASH_H_
