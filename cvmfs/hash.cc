/**
 * \file hash.cc
 * \namespace hash
 *
 * This provides a bit syntactic sugar around the hash algorithms.
 * In particular, hashes can easily be created by constructors.
 * Also, we have a little to-string-from-string conversion.
 *
 * Developed by Jakob Blomer 2009 at CERN
 * jakob.blomer@cern.ch
 */

#include "cvmfs_config.h"
#include "hash.h"

#include <string>
#include <cstring>

using namespace std;

namespace hash {

  static inline void md5_init(MD5_CTX *pms)
  {
    MD5_Init(pms);
  }

  static inline void md5_append(MD5_CTX *pms, const unsigned char *buf, int len)
  {
    MD5_Update(pms, buf, len);
  }

  static inline void md5_finish(MD5_CTX *pms, unsigned char digest[16])
  {
    MD5_Final(digest, pms);
  }

  void sha1_init(SHA_CTX *ctx) {
    SHA1_Init(ctx);
  }

  void sha1_update(SHA_CTX *ctx, const unsigned char *buf, unsigned len) {
    SHA1_Update(ctx, buf, len);
  }


  void sha1_final(unsigned char digest[20], SHA_CTX *ctx) {
    SHA1_Final(digest, ctx);
  }

  static void sha1_string(const unsigned char digest[20], char sha1_str[41])
  {
    int i;
    for(i=0; i<20; ++i) {
      char dgt1 = (unsigned)digest[i] / 16;
      char dgt2 = (unsigned)digest[i] % 16;
      dgt1 += (dgt1 <= 9) ? '0' : 'a' - 10;
      dgt2 += (dgt2 <= 9) ? '0' : 'a' - 10;
      sha1_str[i*2] = dgt1;
      sha1_str[i*2+1] = dgt2;
      //sprintf(&sha1_str[i*2],"%02x",(unsigned)digest[i]);
    }
    sha1_str[40] = 0;
  }

  void sha1_mem(const void *buf, const unsigned buf_size,
                unsigned char digest[20])
  {
    SHA_CTX ctx;
    sha1_init(&ctx);
    sha1_update(&ctx, (const unsigned char *)buf, buf_size);
    sha1_final(digest, &ctx);
  }

  static void sha1_file_fp(FILE *fp, unsigned char digest[20])
  {
    SHA_CTX context;
    int actual;
    unsigned char buffer[4096];

    sha1_init(&context);
    while ((actual = fread(buffer, 1, 4096, fp))) {
      sha1_update(&context, buffer, actual);
    }
    sha1_final(digest, &context);
  }


  int sha1_file(const char *filename, unsigned char digest[20])
  {
    FILE *file;

    file = fopen(filename, "rb");
    if(!file) return 1;

    //  pmesg(D_HASH, "checksumming file %s", filename);
    sha1_file_fp(file, digest);

    fclose(file);
    return 0;
  }



   t_md5::t_md5(const string &str) {
      MD5_CTX md5_state;
      md5_init(&md5_state);
      md5_append(&md5_state, reinterpret_cast<const unsigned char *>(&str[0]), str.length());
      md5_finish(&md5_state, this->digest);
   }

   bool t_md5::operator ==(const t_md5 &other) const {
      // evil hack to make it fast
      // interpret the 128 bit digest as two 64 bit integers and compare them
      int64_t part1 =      (int64_t) *( (int64_t*)(digest + 0) );
      int64_t part2 =      (int64_t) *( (int64_t*)(digest + 8) );
      int64_t otherPart1 = (int64_t) *( (int64_t*)(other.digest + 0) );
      int64_t otherPart2 = (int64_t) *( (int64_t*)(other.digest + 8) );

      return (part1 == otherPart1 && part2 == otherPart2);
   }

   string t_md5::to_string() const {
      string result;
      for (int i = 0; i < 16; ++i) {
         char dgt1 = (unsigned)digest[i] / 16;
         char dgt2 = (unsigned)digest[i] % 16;
         dgt1 += (dgt1 <= 9) ? '0' : 'a' - 10;
         dgt2 += (dgt2 <= 9) ? '0' : 'a' - 10;
         result += dgt1;
         result += dgt2;
      }
      return result;
   }


   t_sha1::t_sha1(const void * const buf_digest, const int buf_size) {
      const int num_bytes = buf_size > 20 ? 20 : buf_size;
      memcpy(digest, buf_digest, num_bytes);
   }


   t_sha1::t_sha1(const std::string &value) {
      SHA_CTX ctx;
      sha1_init(&ctx);
      sha1_update(&ctx, reinterpret_cast<const unsigned char *>(&value[0]), value.length());
      sha1_final(this->digest, &ctx);
   }


   void t_sha1::from_hash_str(const string &sha1_str) {
      if (sha1_str.length() < 40) return;
      for (int i = 0; i < 40; i += 2)
         this->digest[i/2] = (sha1_str[i] <= '9' ? sha1_str[i] -'0' : sha1_str[i] - 'a' + 10)*16 +
                             (sha1_str[i+1] <= '9' ? sha1_str[i+1] - '0' : sha1_str[i+1] - 'a' + 10);
   }

   string t_sha1::to_string() const {
      char sha1_str[41];
      sha1_string(digest, sha1_str);
      return string(sha1_str);
   }

   bool t_sha1::is_null() const {
      for (int i = 0; i < 20; ++i)
         if (digest[i] != 0)
            return false;
      return true;
   }

   bool t_sha1::operator ==(const t_sha1 &other) const {
      //t_sha1 s1 = *this;
      //t_sha1 s2 = other;
      //pmesg(D_HASH, "compare (==) %s and %s", s1.to_string().c_str(), s2.to_string().c_str());
      for (int i = 0; i < 20; i++)
         if (this->digest[i] != other.digest[i])
            return false;
      return true;
   }

   bool t_sha1::operator !=(const t_sha1 &other) const {
      //t_sha1 s1 = *this;
      //t_sha1 s2 = other;
      //pmesg(D_HASH, "compare (!=) %s and %s", s1.to_string().c_str(), s2.to_string().c_str());
      return !(*this == other);
   }

   bool t_sha1::operator <(const t_sha1 &other) const {
      //t_sha1 s1 = *this;
      //t_sha1 s2 = other;
      //pmesg(D_HASH, "compare (<) %s and %s", s1.to_string().c_str(), s2.to_string().c_str());
      for (int i = 0; i < 20; i++) {
         if (this->digest[i] > other.digest[i])
            return false;
         if (this->digest[i] < other.digest[i])
            return true;
      }
      //pmesg(D_HASH, "identical");
      return false;
   }

   bool t_sha1::operator >(const t_sha1 &other) const {
      for (int i = 0; i < 20; i++) {
         if (this->digest[i] < other.digest[i])
            return false;
         if (this->digest[i] > other.digest[i])
            return true;
      }
      return false;
   }


  string MakePath(const t_sha1 &hash, const unsigned dir_levels,
                  const unsigned bytes_per_level)
  {
    const unsigned string_length = t_sha1::CHAR_SIZE + dir_levels + 1;
    char result[string_length];

    unsigned i = 0, pos = 0;
    while (i < t_sha1::CHAR_SIZE) {
      if (((i % bytes_per_level) == 0) &&
          ((i / bytes_per_level) <= dir_levels))
      {
        result[pos] = '/';
        ++pos;
      }
      char digit = ((i % 2) == 0) ? hash.digest[i/2] / 16 :
                                    hash.digest[i/2] % 16;
      digit += (digit <= 9) ? '0' : 'a' - 10;
      result[pos] = digit;
      ++pos;
      ++i;
    }

    return string(result, string_length);
  }

}
