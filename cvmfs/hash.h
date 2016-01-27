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
#include <cctype>
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
  kShake128,  // with 160 output bits
  kAny,
};

/**
 * NOTE: when adding a suffix here, one must edit `cvmfs_swissknife scrub`
 *       accordingly, that checks for invalid hash suffixes
 */
const char kSuffixNone         = 0;
const char kSuffixCatalog      = 'C';
const char kSuffixHistory      = 'H';
const char kSuffixMicroCatalog = 'L';  // currently unused
const char kSuffixPartial      = 'P';
const char kSuffixTemporary    = 'T';
const char kSuffixCertificate  = 'X';
const char kSuffixMetainfo     = 'M';


/**
 * Corresponds to Algorithms.  "Any" is the maximum of all the other
 * digest sizes.
 * When the maximum digest size changes, the memory layout of DirectoryEntry and
 * PosixQuotaManager::LruCommand changes, too!
 */
const unsigned kDigestSizes[] =
  {16,  20,   20,     20,       20};
// Md5  Sha1  Rmd160  Shake128  Any
const unsigned kMaxDigestSize = 20;

/**
 * Hex representations of hashes with the same length need a suffix
 * to be distinguished from each other.  They should all have one but
 * for backwards compatibility MD5 and SHA-1 have none.  Initialized in hash.cc
 * like const char *kAlgorithmIds[] = {"", "", "-rmd160", ...
 */
extern const char *kAlgorithmIds[];
const unsigned kAlgorithmIdSizes[] =
  {0,   0,    7,       9,         0};
// Md5  Sha1  -rmd160  -shake128  Any
const unsigned kMaxAlgorithmIdentifierSize = 9;

/**
 * Corresponds to Algorithms.  There is no block size for Any.
 * Is an HMAC for SHAKE well-defined?
 */
const unsigned kBlockSizes[] =
  {64,  64,   64,     168};
// Md5  Sha1  Rmd160  Shake128

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
         const unsigned char *digest_buffer,
         const Suffix s = kSuffixNone) :
    algorithm(a), suffix(s)
  {
    memcpy(digest, digest_buffer, kDigestSizes[a]);
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

  /**
   * Generates a hexified repesentation of the digest including the identifier
   * string for newly added hashes.
   *
   * @param with_suffix  append the hash suffix (C,H,X, ...) to the result
   * @return             a string representation of the digest
   */
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

  /**
   * Generates a hexified repesentation of the digest including the identifier
   * string for newly added hashes.  Output is in the form of
   * 'openssl x509 fingerprint', e.g. 00:AA:BB:...-SHAKE128
   *
   * @param with_suffix  append the hash suffix (C,H,X, ...) to the result
   * @return             a string representation of the digest
   */
  std::string ToFingerprint(const bool with_suffix = false) const {
    Hex hex(this);
    const bool     use_suffix  = with_suffix && HasSuffix();
    const unsigned string_length =
      hex.length() + kDigestSizes[algorithm] - 1 + use_suffix;
    std::string result(string_length, 0);

    unsigned l = hex.length();
    for (unsigned int hex_i = 0, result_i = 0; hex_i < l; ++hex_i, ++result_i) {
      result[result_i] = toupper(hex[hex_i]);
      if ((hex_i < 2 * kDigestSizes[algorithm] - 1) && (hex_i % 2 == 1)) {
        result[++result_i] = ':';
      }
    }

    if (use_suffix) {
      result[string_length - 1] = suffix;
    }

    assert(result.length() == string_length);
    return result;
  }

  /**
   * Convenience method to generate a string representation of the digest.
   * See Digest<>::ToString() for details
   *
   * @return  a string representation including the hash suffix of the digest
   */
  std::string ToStringWithSuffix() const {
    return ToString(true);
  }

  /**
   * Generate the standard relative path from the hexified digest to be used in
   * CAS areas or cache directories. Throughout the entire system we use one
   * directory level (first to hex digest characters) for namespace splitting.
   * Note: This method appends the internal hash suffix to the path.
   *
   * @return  a relative path representation of the digest including the suffix
   */
  std::string MakePath() const {
    return MakePathExplicit(1, 2, suffix);
  }

  /**
   * The alternative path is used to symlink the root catalog from the webserver
   * root to the data directory.  This way, the data directory can be protected
   * while the root catalog remains accessible.
   */
  std::string MakeAlternativePath() const {
    return ".cvmfsalt-" + ToStringWithSuffix();
  }

  /**
   * Produces a relative path representation of the digest without appending the
   * hash suffix. See Digest<>::MakePath() for more details.
   *
   * @return  a relative path representation of the digest without the suffix
   */
  std::string MakePathWithoutSuffix() const {
    return MakePathExplicit(1, 2, kSuffixNone);
  }

  /**
   * Generates an arbitrary path representation of the digest. Both number of
   * directory levels and the hash-digits per level can be customized. Further-
   * more an arbitrary hash suffix can be provided.
   * Note: This method is mainly meant for internal usage but stays public for
   *       historical reasons.
   *
   * @param dir_levels        the number of namespace splitting directory levels
   * @param digits_per_level  each directory level's number of hex-digits
   * @param hash_suffix       the hash suffix character to be appended
   * @return                  a relative path representation of the digest
   */
  std::string MakePathExplicit(const unsigned dir_levels,
                               const unsigned digits_per_level,
                               const Suffix   hash_suffix = kSuffixNone) const {
    Hex hex(this);

    // figure out how big the output string needs to be
    const bool use_suffix = (hash_suffix != kSuffixNone);
    const unsigned string_length = hex.length() + dir_levels + use_suffix;
    std::string result;
    result.resize(string_length);

    // build hexified hash and path delimiters
    unsigned i   = 0;
    unsigned pos = 0;
    for (; i < hex.length(); ++i) {
      if (i > 0 && (i % digits_per_level == 0)
                && (i / digits_per_level <= dir_levels)) {
        result[pos++] = '/';
      }
      result[pos++] = hex[i];
    }

    // (optionally) add hash hint suffix
    if (use_suffix) {
      result[pos++] = hash_suffix;
    }

    assert(i   == hex.length());
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
struct Shake128 : public Digest<20, kShake128> { };

/**
 * Any as such must not be used except for digest storage.
 * To do real work, the class has to be "blessed" to be a real hash by
 * setting the algorithm field accordingly.
 */
struct Any : public Digest<kMaxDigestSize, kAny> {
  Any() : Digest<kMaxDigestSize, kAny>() { }

  explicit Any(const Algorithms a,
               const char       s = kSuffixNone) :
    Digest<kMaxDigestSize, kAny>() { algorithm = a; suffix = s; }

  Any(const Algorithms     a,
      const unsigned char *digest_buffer,
      const Suffix         suffix = kSuffixNone) :
    Digest<kMaxDigestSize, kAny>(a, digest_buffer, suffix) { }

  explicit Any(const Algorithms  a,
               const HexPtr      hex,
               const char        suffix = kSuffixNone) :
    Digest<kMaxDigestSize, kAny>(a, hex, suffix) { }
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
bool HashFile(const std::string &filename, Any *any_digest);
bool HashFd(int fd, Any *any_digest);
void HashMem(const unsigned char *buffer, const unsigned buffer_size,
             Any *any_digest);
void HashString(const std::string &content, Any *any_digest);
void Hmac(const std::string &key,
          const unsigned char *buffer, const unsigned buffer_size,
          Any *any_digest);


Algorithms ParseHashAlgorithm(const std::string &algorithm_option);
Any MkFromHexPtr(const HexPtr hex, const Suffix suffix = kSuffixNone);

}  // namespace shash

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_HASH_H_
