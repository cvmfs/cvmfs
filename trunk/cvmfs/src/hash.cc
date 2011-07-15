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

#include "config.h"
#include "hash.h"

#include <string>
#include <cstring>

extern "C" {
   #include "md5.h"
   #include "sha1.h"
   #include "debug.h"
}

using namespace std;

namespace hash { 

   t_md5::t_md5(const string &str) {
      md5_state_t md5_state; 
      md5_init(&md5_state);
      md5_append(&md5_state, reinterpret_cast<const unsigned char *>(&str[0]), str.length());
      md5_finish(&md5_state, this->digest);
   }
   
   bool t_md5::operator ==(const t_md5 &other) const {
      for (int i = 0; i < 16; i++)
         if (this->digest[i] != other.digest[i])
            return false;
      return true;
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
      sha1_context_t ctx;
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
   
}
