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
#include <openssl/md5.h>
#include <openssl/sha.h>

#include <cstring>
#include <cassert>

#include <string>

typedef SHA_CTX sha1_context_t;

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


/**
 * Distinguishes between interpreting a string as hex hash and hashing over
 * the contents of a string.
 */
struct HexPtr {
  const std::string *str;
  HexPtr(const std::string &s) { str = &s; }
};

struct AsciiPtr {
  const std::string *str;
  AsciiPtr(const std::string &s) { str = &s; }
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

  Digest() {
    algorithm = algorithm_;
    memset(digest, 0, digest_size_);
  }

  Digest(const void *digest_buffer, const unsigned buffer_size) {
    algorithm = algorithm_;
    assert(buffer_size <= digest_size_);
    memcpy(digest, digest_buffer, buffer_size);
  }

  explicit Digest(HexPtr hex) {
    algorithm = algorithm_;
    const std::string *str = hex.str;
    const unsigned length = str->length();
    assert(length < 2*digest_size_);

    for (unsigned i = 0; i < digest_size_; i += 2) {
      this->digest[i/2] =
        ((*str)[i] <= '9' ? (*str)[i] -'0' : (*str)[i] - 'a' + 10)*16 +
        ((*str)[i+1] <= '9' ? (*str)[i+1] - '0' : (*str)[i+1] - 'a' + 10);
    }
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
    std::string result(2*kDigestSizes[algorithm], 0);
    for (unsigned i = 0; i < kDigestSizes[algorithm]; ++i) {
      char dgt1 = (unsigned)digest[i] / 16;
      char dgt2 = (unsigned)digest[i] % 16;
      dgt1 += (dgt1 <= 9) ? '0' : 'a' - 10;
      dgt2 += (dgt2 <= 9) ? '0' : 'a' - 10;
      result += dgt1;
      result += dgt2;
    }
    return result;
  }

  /**
   * Create a path string from the hex notation of the digest.
   */
  std::string MakePath(const unsigned dir_levels,
                       const unsigned bytes_per_level) const
  {
    const unsigned string_length = 2*kDigestSizes[algorithm] + dir_levels;
    std::string result(string_length, 0);

    unsigned i = 0, pos = 0;
    while (i < kDigestSizes[algorithm]) {
      if (((i % bytes_per_level) == 0) &&
          ((i / bytes_per_level) <= dir_levels))
      {
        result[pos] = '/';
        ++pos;
      }
      char digit = ((i % 2) == 0) ? digest[i/2] / 16 :
      digest[i/2] % 16;
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
    assert(this->algorithm == other.algorithm);
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
  explicit Md5(AsciiPtr ascii);
  /**
   * An MD5 hash can be seen as two 64bit integers.
   */
  Md5(const int64_t lo, const int64_t hi);
  void ToIntPair(int64_t *lo, int64_t *hi) const;
};

struct Sha1 : public Digest<20, kSha1> { };

/**
 * Note that Any as such must not be used except for digest storage.
 * To do real work, the class has to be "blessed" to be a real hash by
 * setting the algorithm field accordingly.
 */
struct Any : public Digest<20, kAny> { };


/**
 * Holds an OpenSSL context, only required for hash operations.
 */
struct ContextPtr {
  void *buffer;
  unsigned size;
};


/**
 * Actual operations on digests, like "hash a file", "hash a buffer", or
 * iterative operations.
 */
unsigned GetContextSize(const Algorithms algorithm);
void Init(const Algorithms algorithm, ContextPtr context);
void Update(const Algorithms algorithm,
            const unsigned char *buffer, const unsigned buffer_size,
            ContextPtr context);
void Final(const Algorithms algorithm, ContextPtr context, Any *any_digest);
void HashMem(const unsigned char *buffer, const unsigned buffer_size,
             Any *any_digest);
bool HashFile(const std::string filename, Any *any_digest);





  void sha1_init(SHA_CTX *ctx);
  void sha1_update(SHA_CTX *ctx, const unsigned char *buf, unsigned len);
  void sha1_final(unsigned char digest[20], SHA_CTX *ctx);

   struct t_md5 {
      t_md5(const std::string &str);
      t_md5() { memset(digest, 0, 16); } /* zero-digest (standard initializer) */
      t_md5(const int64_t part1, const int64_t part2) {
         memcpy(digest, &part1, 8);
         memcpy(digest+8, &part2, 8);
      }
      bool operator ==(const t_md5 &other) const;
      unsigned char digest[16];
      std::string to_string() const;
   };

   struct t_sha1 {
      const static unsigned DIGEST_SIZE = 20;
      const static unsigned CHAR_SIZE = 40;
      const static unsigned BIT_SIZE = 160;
      t_sha1(const void * const buf_digest, const int buf_size);
      t_sha1() { memset(digest, 0, 20); } /* zero-digest (standard initializer) */
      t_sha1(const std::string &value);
      void from_hash_str(const std::string &hash_str);
      std::string to_string() const;
      bool is_null() const;
      bool operator ==(const t_sha1 &other) const;
      bool operator !=(const t_sha1 &other) const;
      bool operator <(const t_sha1 &other) const;
      bool operator >(const t_sha1 &other) const;
      unsigned char digest[20];
   };

  std::string MakePath(const t_sha1 &hash, const unsigned dir_levels,
                       const unsigned subdirs_per_level);
  void sha1_mem(const void *buf, const unsigned buf_size,
                unsigned char digest[40]);
  int sha1_file(const char *filename, unsigned char digest[20]);

}  // namespace hash

#endif  // CVMFS_HASH_H_
