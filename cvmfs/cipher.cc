#include <openssl/pkcs7.h>
#include <openssl/bio.h>

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/err.h>
#include <openssl/bio.h>
#include <openssl/rsa.h>
#include <openssl/engine.h>
#include <openssl/cms.h>

#include <string>
#include <vector>

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cctype>
#include <iostream>

#include "cipher.h"
using namespace std;  // NOLINT

namespace cipher {

X509 *certificate_ = NULL;
EVP_PKEY *private_key_ = NULL;
EVP_CIPHER_CTX ctx;
unsigned char* aes_key_ = NULL;
char iv_[33];


/**
 * Loads a certificate.  This certificate is used for the following
 * signature verifications
 *
 * \return True on success, false otherwise
 */
bool LoadCertificatePath(const string &file_pem) {
  /*from signature.cc*/
  if (certificate_) {
    X509_free(certificate_);
    certificate_ = NULL;
  }

  bool result;
  char *nopwd = strdupa("");
  FILE *fp;

  if ((fp = fopen(file_pem.c_str(), "r")) == NULL)
    return false;
  result = (certificate_ = PEM_read_X509_AUX(fp, NULL, NULL, nopwd)) != NULL;

  if (!result && certificate_) {
    X509_free(certificate_);
    certificate_ = NULL;
  }

  fclose(fp);
  return result;
}

bool LoadPrivateKeyPath(const string &file_pem, const string &password) {
  /*from signature.cc*/
  bool result;
  FILE *fp = NULL;
  char *tmp = strdupa(password.c_str());

  if ((fp = fopen(file_pem.c_str(), "r")) == NULL)
    return false;
  result = (private_key_ = PEM_read_PrivateKey(fp, NULL, NULL, tmp)) != NULL;
  fclose(fp);
  return result;
} 

bool LoadAESKey() {
  // Decrypt the key from PKCS7

  OpenSSL_add_all_algorithms();
  ERR_load_crypto_strings();

  BIO *bio_out;
 

  PKCS7 *p7 = NULL;
  BIO *in; 
  char buf[300];
  void* bio_buffer;
  int buffer_size;

  BIO *cont = NULL;
  CMS_ContentInfo *cms = NULL;
  const char *data =
    "-----BEGIN PKCS7-----\n"
"MIIBUAYJKoZIhvcNAQcDoIIBQTCCAT0CAQAxggEBMIH+AgEAMGcwWTESMBAGCgmS\n"
"JomT8ixkARkWAmNoMRQwEgYKCZImiZPyLGQBGRYEY2VybjEtMCsGA1UEAxMkQ0VS\n"
"TiBUcnVzdGVkIENlcnRpZmljYXRpb24gQXV0aG9yaXR5AgoW9OwmAAIAATqqMA0G\n"
"CSqGSIb3DQEBAQUABIGAgHcIrlz7ix5xsg6ofZPGNshjikUMewK0XT+fNNsy1eOO\n"
"WZOsXYqT0VzlPdwOsb5W9OlMKbE4FhjY8FdM5DV/3M+x0aGmUdZL9c13C2VX+3yd\n"
"+EsJra2u5/1JhJxHaDtSZt1tluomWOUEmqKStiDx/VPGEExMjvENq61yh86spZow\n"
"MwYJKoZIhvcNAQcBMBQGCCqGSIb3DQMHBAj+1YCEOx7a2IAQuXyrZfgQGHOmK1+R\n"
"xBsTLg==\n"
"-----END PKCS7-----\n";  // TODO: retrieve the real key


  bio_out = BIO_new(BIO_s_mem());
  if (!bio_out) printf("error\n"); 
 
  //  PKCS7
  in = BIO_new(BIO_s_mem());
  BIO_puts(in, data);  //  load the pkcs7 file in the bio
 
  if (!PEM_read_bio_PKCS7(in, &p7, 0, NULL))
    printf("error reading pkcs7\n");
 
  if (!PKCS7_decrypt(p7, private_key_, certificate_, bio_out, 0)) {
    ERR_print_errors_fp(stderr);
    return false;
  }

  //  Store the key
  buffer_size = BIO_get_mem_data(bio_out, &bio_buffer);
  aes_key_ = (unsigned char *)malloc(buffer_size);
  memcpy(aes_key_, bio_buffer, buffer_size);
  BIO_free(bio_out);

  //  printf("%s", aes_key_);

  return true;
  }

bool LoadIV(const string &iv) { 

  strcpy(iv_ , iv.c_str());
 } 

bool Decrypt(const unsigned char *buffer,
              const unsigned buffer_size)
{
  /*  
  //char* key;
  char* iv;

  char* buf_out; //buffer for clear-text string
  int byte_out; //decoded bytes
  int byte_outF; //last decoded bytes

  buf_out = (char*)malloc(sizeof(char)*(buffer_size + EVP_CIPHER_CTX_block_size())); //alloc enough space for padding

  // Initialize decryption
  EVP_DecryptInit(&ctx, EVP_aes_cbc(), aes_key_, iv_);

  // Decrypt
  EVP_DecryptUpdate(ctx, buf_out, &byte_out, buffer, buffer_size);

  // Finalize
  if(!EVP_DecryptFinal(ctx, &buf_out[byte_out], &byte_outF))
    printf("Padding incorrect\n");
  */

}


} //namespace cipher
