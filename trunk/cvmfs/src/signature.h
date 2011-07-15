
#ifndef _SIGNATURE_H
#define _SIGNATURE_H 1

#include "hash.h"
#include <string>
#include <stdio.h>

namespace signature {
   
   void init();
   void fini();
   
   bool load_private_key(const std::string file_pem, const std::string password);
   void unload_private_key();
   bool load_ca2chain(const std::string file_pem);
   //bool load_crl2chain(std::string file_pem);
   bool load_certificate(const std::string file_pem, const bool verify);
   bool load_certificate(const void *buf, const unsigned buf_size, const bool verify);
   bool load_public_key(const std::string file_pem);
   std::string fingerprint(); /* use only when certificate is loaded */
   bool write_certificate(const std::string file_pem);
   bool write_certificate(void **buf, unsigned *buf_size);
   bool keys_match();
   std::string whois();
   
   bool sign(const void *buf, const unsigned size_buf, void **sig, unsigned *sig_size);
   bool verify(const void *buf, const unsigned buf_size, const void *sig, unsigned sig_size);
   bool verify(const void *buf, const unsigned buf_size, const std::string file_sig);
   bool verify_rsa(const void *buf, const unsigned buf_size, const void *sig, unsigned sig_size);
   
   std::string get_crypto_err();
}

#endif
