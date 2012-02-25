#ifndef HASH_H
#define HASH_H 1

#include <string>
#include <cstring>
#include <openssl/md5.h>
#include <openssl/sha.h>

typedef SHA_CTX sha1_context_t;

namespace hash {

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
      const static unsigned DIGEST_SIZE = 40;
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
}


#endif
