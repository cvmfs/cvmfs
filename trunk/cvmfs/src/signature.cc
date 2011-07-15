/**
 * \file signature.cc
 * \namespace signature
 *
 * This is wrapper around OpenSSL's libcrypto.  It supports
 * signing of data with an X.509 certificate and verifiying
 * a signature against a certificate.
 *
 * Certificates itself can be verified according to a CA-chain.
 *
 * We work exclusively with PEM formatted files (= Base64-encoded DER files).
 *
 * Developed by Jakob Blomer 2009 at CERN
 * jakob.blomer@cern.ch
 */

#include "config.h"
#include "signature.h"

#include "hash.h"

#include <string>

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cctype>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/err.h>
#include <openssl/bio.h>
#include <openssl/rsa.h>
#include <openssl/engine.h>

extern "C" {
   #include "sha1.h"
   #include "smalloc.h"
}

using namespace std;

namespace signature {
   
   EVP_PKEY *privkey = NULL;
   X509 *certificate = NULL;
   X509_STORE *ca_store = NULL; ///< Contains the CA-Chain until the self-signed root
   RSA *pubkey = NULL; ///< Contains cvmfs master public key
   
   void init() {
      OpenSSL_add_all_algorithms();
      ca_store = X509_STORE_new();
      X509_STORE_set_default_paths(ca_store);
   }
   
   
   void fini() {
      EVP_cleanup();
      if (ca_store) X509_STORE_free(ca_store);
      if (certificate) X509_free(certificate);
      if (privkey) EVP_PKEY_free(privkey);
      if (pubkey) RSA_free(pubkey);
   }
   
   
   /**
    * @param[in] file_pem File name of the PEM key file
    * @param[in] password Password for the private key. 
    *     Password is not saved internally, but the private key is.
    * \return True on success, false otherwise
    */
   bool load_private_key(const string file_pem, const string password) {
      bool result;
      FILE *fp = NULL;
      char *tmp = strdupa(password.c_str());
      
      if ((fp = fopen(file_pem.c_str(), "r")) == NULL)
         return false;
      result = (privkey = PEM_read_PrivateKey(fp, NULL, NULL, tmp)) != NULL;
      fclose(fp);
      return result;
   }
   
   
   /**
    * Clears the memory storing the private key
    */
   void unload_private_key() {
      if (privkey) EVP_PKEY_free(privkey);
      privkey = NULL;
   }
   
   
   /**
    * Loads a certificate of a CA to the CA-chain.
    * Don't use catted-PEM files here, only one CA per file.
    *
    * \return True on success, false otherwise
    */
   bool load_ca2chain(const string file_pem) {
      if (ca_store == NULL)
         return false;
         
      bool result;
      char *tmp = strdupa("");
      FILE *fp = NULL;
      X509 *ca_cert;
      
      if ((fp = fopen(file_pem.c_str(), "r")) == NULL)
         return false;

      if ((ca_cert = PEM_read_X509_AUX(fp, NULL, NULL, tmp)) == NULL)
         result = false;
      else {
         result = X509_STORE_add_cert(ca_store, ca_cert);
         if (!result) {
            X509_free(ca_cert);
            result = false;
         }
      }
      fclose(fp);
      return result;
   }
   
/*   bool load_crl2chain(string file_pem) {
      if (ca_store == NULL)
         return false;
         
      bool result;
      char *tmp = strdupa("");
      FILE *fp = NULL;
      X509_CRL *crl;
      
      if ((fp = fopen(file_pem.c_str(), "r")) == NULL)
         return false;

      if ((crl = PEM_read_X509_CRL(fp, NULL, NULL, tmp)) == NULL)
         result = false;
      else {
         result = X509_STORE_add_crl(ca_store, crl);
         if (!result) {
            X509_free(crl);
            result = false;
         }
      }
      fclose(fp);
      return result;
   }*/
   
   
   /**
    * Loads the current active certificate.  This certificate is used
    * for signature verification.
    *
    * @param[in] verify Specifies, if the certificate itself shall be verified against CA-chain
    * \return True on success, false otherwise
    */
   bool load_certificate(const string file_pem, const bool verify) {
      if (certificate) {
         X509_free(certificate);
         certificate = NULL;
      }
         
      bool result;
      char *nopwd = strdupa("");
      FILE *fp;
      
      if ((fp = fopen(file_pem.c_str(), "r")) == NULL)
         return false;
      if ((certificate = PEM_read_X509_AUX(fp, NULL, NULL, nopwd)) == NULL)
         result = false;
      else {
         if (verify) {
            X509_STORE_CTX csc;
            X509_STORE_CTX_init(&csc, ca_store, certificate, NULL);
            result = X509_verify_cert(&csc) != 0;
            X509_STORE_CTX_cleanup(&csc);
         } else
            result = true;
      }
      
      if (!result && certificate) {
         X509_free(certificate);
         certificate = NULL;
      }
      
      fclose(fp);
      return result;
   }
   
   /**
    * See the function that loads the certificate from file.
    */
   bool load_certificate(const void *buf, const unsigned buf_size, const bool verify) {
      if (certificate) {
         X509_free(certificate);
         certificate = NULL;
      }
         
      bool result;
      char *nopwd = strdupa("");
      
      BIO *mem = BIO_new(BIO_s_mem());
      if (!mem) return false;
      if (BIO_write(mem, buf, buf_size) <= 0) {
         BIO_free(mem);
         return false;
      }
      if ((certificate = PEM_read_bio_X509_AUX(mem, NULL, NULL, nopwd)) == NULL) {
         result = false;
      } else {
         if (verify) {
            X509_STORE_CTX csc;
            X509_STORE_CTX_init(&csc, ca_store, certificate, NULL);
            result = X509_verify_cert(&csc) != 0;
            X509_STORE_CTX_cleanup(&csc);
         } else
            result = true;
      }
      BIO_free(mem);
      
      if (!result && certificate) {
         X509_free(certificate);
         certificate = NULL;
      }
      
      return result;
   }
   
   /**
    * Loads a public RSA key.
    */
   bool load_public_key(const string file_pem) {
      if (pubkey) {
         RSA_free(pubkey);
         pubkey = NULL;
      }
         
      char *nopwd = strdupa("");
      FILE *fp;
      
      if ((fp = fopen(file_pem.c_str(), "r")) == NULL)
         return false;
      EVP_PKEY *k;
      if ((k = PEM_read_PUBKEY(fp, NULL, NULL, nopwd)) == NULL) {
         fclose(fp);
         return false;
      }
      fclose(fp);
      pubkey = EVP_PKEY_get1_RSA(k);
      EVP_PKEY_free(k);
      
      return pubkey != NULL;
   }
   
   /**
    * Returns sha1 hash from DER encoded certificate, 
    * encoded as openssl does (01:AB:...).
    * Empty string on failure.
    *
    * Certificate has to be already loaded.
    */
   string fingerprint() {
      if (!certificate) return "";
      
      int buf_size;
      unsigned char *buf = NULL;

      buf_size = i2d_X509(certificate, &buf);
      if (buf_size < 0) return "";

      hash::t_sha1 sha1;
      sha1_mem(buf, (unsigned)buf_size, sha1.digest);
      free(buf);
      
      const string sha1_str = sha1.to_string();
      string result;
      for (unsigned i = 0; i < sha1_str.length(); ++i) {
         if ((i > 0) && (i%2 == 0)) result += ":";
         result += toupper(sha1_str[i]);
      }
      return result;
   }
   
   /**
    * \return Some information about the loaded certificate.
    */
   string whois() {
      if (!certificate)
         return "No certificate loaded";
      
      string result;
      X509_NAME *subject = X509_get_subject_name(certificate);
      X509_NAME *issuer = X509_get_issuer_name(certificate);
      char *buf = NULL;
      buf = X509_NAME_oneline(subject, NULL, 0);
      if (buf) {
         result = "Publisher: " + string(buf);
         free(buf);
      }
      buf = X509_NAME_oneline(issuer, NULL, 0);
      if (buf) {
         result += "\nCertificate issued by: " + string(buf);
         free(buf);
      }
      return result;
   }
   
   
   /**
    * Writes the currently loaded certificate into a PEM file.
    *
    * \return True on success, false otherwise
    */
   bool write_certificate(const string file_pem) {
      int result;
      FILE *fp;
      
      if (!certificate)
         return false;
      if ((fp = fopen(file_pem.c_str(), "w")) == NULL)
         return false;
      result = PEM_write_X509(fp, certificate);
      fclose(fp);
      return result;
   }
   
   
   bool write_certificate(void **buf, unsigned *buf_size) {
      BIO *mem = BIO_new(BIO_s_mem());
      if (!mem) return false;
      if (!PEM_write_bio_X509(mem, certificate)) {
         BIO_free(mem);
         return false;
      }
      
      void *bio_buf;
      *buf_size = BIO_get_mem_data(mem, &bio_buf);
      *buf = smalloc(*buf_size);
      memcpy(*buf, bio_buf, *buf_size);
      BIO_free(mem);
      return true;      
   }
   
   
   /**
    * Checks, whether the loaded certificate and the loaded private key match.
    *
    * \return True, if private key and certificate match, false otherwise.
    */
   bool keys_match() {
      if (!certificate || !privkey)
         return false;
         
      bool result = false;
      char buf[] = "test";
      void *sig = NULL;
      unsigned sig_size;
      if (sign(buf, 5, &sig, &sig_size) &&
          verify(buf, 5, sig, sig_size))
      {
         result = true;
      }
      if (sig) free(sig);
      return result;
   }
   
   
   /**
    * Signs a data block using the loaded private key.
    *
    * \return True on sucess, false otherwise
    */
   bool sign(const void *buf, const unsigned buf_size, void **sig, unsigned *sig_size) {
      if (!privkey) {
         sig_size = 0;
         return false;
      }
         
      bool result = false;
      EVP_MD_CTX ctx;
      
      *sig = smalloc(EVP_PKEY_size(privkey));
      EVP_MD_CTX_init(&ctx);
      if (EVP_SignInit(&ctx, EVP_sha1()) &&
          EVP_SignUpdate(&ctx, buf, buf_size) &&
          EVP_SignFinal(&ctx, (unsigned char *)*sig, sig_size, privkey))
      {
         result = true;
      }
      EVP_MD_CTX_cleanup(&ctx);
      if (!result) {
         free(*sig);
         *sig_size = 0;
      }

      return result;
   }
   
   
   /**
    * Veryfies a signature against loaded certificate.
    *
    * \return True if signature is valid, false on error or otherwise
    */
   bool verify(const void *buf, const unsigned buf_size, const void *sig, const unsigned sig_size) {
      if (!certificate)
         return false;
         
      bool result = false;
      EVP_MD_CTX ctx;
      
      EVP_MD_CTX_init(&ctx);
      if (EVP_VerifyInit(&ctx, EVP_sha1()) &&
          EVP_VerifyUpdate(&ctx, buf, buf_size) &&
          EVP_VerifyFinal(&ctx, (unsigned char *)sig, sig_size, X509_get_pubkey(certificate)))
      {
         result = true;
      }
      EVP_MD_CTX_cleanup(&ctx);
      
      return result;
   }
   
   
   /**
    * The signature can also be given by a file (see the other verify()).
    */
   bool verify(const void *buf, const unsigned buf_size, const std::string file_sig) {
      bool result;
      FILE *fp = NULL;
      unsigned char *sig_buf[512];
      unsigned sig_size = 0;
      unsigned sig_cap = 512;
      int have;
      
      if ((fp = fopen(file_sig.c_str(), "r")) == NULL)
         return false;
      
      void *sig = smalloc(sig_cap);
      
      do {
         have = fread(sig_buf, 1, 512, fp);
         if (sig_size + have > sig_cap) {
            sig_cap *= 2;
            sig = srealloc(sig, sig_cap);
         }
         memcpy((unsigned char *)sig+sig_size, sig_buf, have);
         sig_size += have;
      } while (have == 512);
      
      result = verify(buf, buf_size, sig, sig_size);
      
      free(sig);
      fclose(fp);
      return result;     
   }
   
   
   /**
    * Veryfies a signature against loaded public key.
    *
    * \return True if signature is valid, false on error or otherwise
    */
   bool verify_rsa(const void *buf, const unsigned buf_size, const void *sig, const unsigned sig_size) {
      if (!pubkey)
         return false;
      if (buf_size > (unsigned)RSA_size(pubkey))
         return false;
      
      unsigned char *to = (unsigned char *)alloca(RSA_size(pubkey));
      unsigned char *from = (unsigned char *)alloca(sig_size);
      memcpy(from, sig, sig_size);
      
      int size;
      if ((size = RSA_public_decrypt(sig_size, from, to, pubkey, RSA_PKCS1_PADDING)) < 0)
         return false;
            
      return ((unsigned)size == buf_size) && (memcmp(buf, to, size) == 0);
   }

   
   
   /**
    * Adds verbosity.
    */
   string get_crypto_err() {
      char buf[121];
      string err;
      //ERR_print_errors_fp(stderr);
      while (ERR_peek_error() != 0) { 
         ERR_error_string(ERR_get_error(), buf);
         err += string(buf);
      } 
      return err;
   }
}
