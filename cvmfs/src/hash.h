#ifndef HASH_H
#define HASH_H 1

#include <string>
#include <cstring>

namespace hash {
   
   struct t_md5 {
      t_md5(const std::string &str);
      t_md5() { memset(digest, 0, 16); } /* zero-digest (standard initializer) */
      bool operator ==(const t_md5 &other) const;
      unsigned char digest[16];
      std::string to_string() const;
   };
   
   struct t_sha1 {
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

}


#endif
