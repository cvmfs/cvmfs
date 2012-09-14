/**
 * This file is part of the CernVM File System.
 *
 * It is needed to load a crypted symmetric key in PKCS7 format, using private keys in PEM.
 * The symmetric key is then used to decrypt files.
 */

#include <openssl/pkcs7.h>
#include <openssl/bio.h>

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/err.h>
#include <openssl/bio.h>
#include <openssl/rsa.h>
#include <openssl/engine.h>

#include <string>
#include <vector>

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cctype>
#include <iostream>

#include "cipher.h"
#include "logging.h"
using namespace std;  // NOLINT

namespace cipher {

X509 *certificate_ = NULL;
EVP_PKEY *private_key_ = NULL;
EVP_CIPHER_CTX *ctx_;
unsigned char* aes_key_ = NULL;
unsigned char* iv_;

void Init(){
  OpenSSL_add_all_algorithms();
  ERR_load_crypto_strings();
}

void Fini(){
}
/**
 * Loads a certificate.  The certificate is needed
 * to decrypt data in PKCS7, together with the private key.
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




  BIO *bio_out;
 

  PKCS7 *p7 = NULL;
  BIO *in; 
  char buf[300];
  void* bio_buffer;
  int buffer_size;

  BIO *cont = NULL;
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
"-----END PKCS7-----\n";  // string "password"

data = "-----BEGIN PKCS7-----\n"
"MIIBWAYJKoZIhvcNAQcDoIIBSTCCAUUCAQAxggEBMIH+AgEAMGcwWTESMBAGCgmS\n"
"JomT8ixkARkWAmNoMRQwEgYKCZImiZPyLGQBGRYEY2VybjEtMCsGA1UEAxMkQ0VS\n"
"TiBUcnVzdGVkIENlcnRpZmljYXRpb24gQXV0aG9yaXR5AgoW9OwmAAIAATqqMA0G\n"
"CSqGSIb3DQEBAQUABIGAhwjfNqjD0qFJc+ZXmEAESLh9kRt5Lq0Vb/nF6UbnUGQ/\n"
"iwN5BNqPV7o1GXcqzJREhAuV3vYSDmfKEGdDurBVjHYuyhrMjvqmyh9i499+qpQB\n"
"n+T3YYypxclwzzeTNEkmtJket5bmMJmNayqP3DM7LjlgWtgAhWsBL0sJq07pAYsw\n"
"OwYJKoZIhvcNAQcBMBQGCCqGSIb3DQMHBAhe6BfmzBtzpoAYXVyuMAUwfSxG/16q\n"
"Qb6mmzKNAnyztayL\n"
  "-----END PKCS7-----\n"; //binary key example

data = "-----BEGIN PKCS7-----\n"
"MIIBaAYJKoZIhvcNAQcDoIIBWTCCAVUCAQAxggEBMIH+AgEAMGcwWTESMBAGCgmS\n"
"JomT8ixkARkWAmNoMRQwEgYKCZImiZPyLGQBGRYEY2VybjEtMCsGA1UEAxMkQ0VS\n"
"TiBUcnVzdGVkIENlcnRpZmljYXRpb24gQXV0aG9yaXR5AgoW9OwmAAIAATqqMA0G\n"
"CSqGSIb3DQEBAQUABIGAZnhJdqPXzoadVY9wQ8uGC6bDgwJlc4p7DfpYduErRMgz\n"
"7dY07WAosEDDw5RDMaCVqCNn3hvDctGAjbxkeTF1YaHOUEwn0rogfRsI+xmHf+rR\n"
"mBIHguMjmd79rfoOl2yfA63J1Vm9LP50XNC17jr49wxTijaSIOntyH/DpOra69ow\n"
"SwYJKoZIhvcNAQcBMBQGCCqGSIb3DQMHBAibDJAdGOagXoAoQEbuQ7uNYM7lIfr6\n"
"za+Ya4AMj17TaqT6OrgIVK4xXrphOpnyYLJObQ==\n"
  "-----END PKCS7-----\n"; // hex string key example

data = "-----BEGIN PKCS7-----\n"
"MIIBiAYJKoZIhvcNAQcDoIIBeTCCAXUCAQAxggEBMIH+AgEAMGcwWTESMBAGCgmS\n"
"JomT8ixkARkWAmNoMRQwEgYKCZImiZPyLGQBGRYEY2VybjEtMCsGA1UEAxMkQ0VS\n"
"TiBUcnVzdGVkIENlcnRpZmljYXRpb24gQXV0aG9yaXR5AgoW9OwmAAIAATqqMA0G\n"
"CSqGSIb3DQEBAQUABIGAWNGKHE6H7OFU9c90uAjiiRP4mR6DFB5cCahKyuBA3lec\n"
"XpbhgOkkA9q+U6LZaT+K0Z31FzIr3gO+zvbp13sPFeqDz1oYpymlGaRe+1DXPNDF\n"
"OtMpsm/c0JK/vxKewzsBjQfOPKkod6tz0sulJMl1X70y4xHSWMw9jfRPt9ZVWuow\n"
"awYJKoZIhvcNAQcBMBQGCCqGSIb3DQMHBAi+fRwENHFhXoBInrqjv5YuQ2IrxC4A\n"
"5nSBExAMN+Tsc33g9NSTHNFQLdJlX7/Rwq6lt0UtnguytxMJV6zjriAPpZVJ5drP\n"
"P1bW82ZTeHxIVwlW\n"
  "-----END PKCS7-----\n"; // 32byte key

data = "-----BEGIN PKCS7-----\n"
"MIIBiAYJKoZIhvcNAQcDoIIBeTCCAXUCAQAxggEBMIH+AgEAMGcwWTESMBAGCgmS\n"
"JomT8ixkARkWAmNoMRQwEgYKCZImiZPyLGQBGRYEY2VybjEtMCsGA1UEAxMkQ0VS\n"
"TiBUcnVzdGVkIENlcnRpZmljYXRpb24gQXV0aG9yaXR5AgoW9OwmAAIAATqqMA0G\n"
"CSqGSIb3DQEBAQUABIGAZ0lrm+r0heqrnMzGfRrJvRHzxuTo9vl0i4YINtjyrNjL\n"
"MWVj276CRMXDErq6XuaVTtJNpgKsqpbPjTXWM4DrM8okrBLtIERK+2ehrw2L9EcW\n"
"hY/7vbCZR10JeJT1KRHbLNfYsvEStHpimnjewXpRDs/1yUUcbIZH5chawaCcEjYw\n"
"awYJKoZIhvcNAQcBMBQGCCqGSIb3DQMHBAj9HX3ZQLomloBIx5fvfwqtKvMWULqP\n"
"5ylJG6oYT1AOqSB1k68Tooml9bcW2yfLee4UnL4Ma4sFeGZlgdN1/VtS+KbfUMv9\n"
"HcWOnet68daOM5X4\n"
  "-----END PKCS7-----\n";
 //another 32 byte key






  bio_out = BIO_new(BIO_s_mem());
  if (!bio_out) return false;
 
  //  PKCS7
  in = BIO_new(BIO_s_mem());
  BIO_puts(in, data);  //  load the pkcs7 file in the bio
 
  if (!PEM_read_bio_PKCS7(in, &p7, 0, NULL)){
    LogCvmfs(kLogCipher, kLogDebug, "error reading pkcs7\n");
    return false;
  }
 
  if (!PKCS7_decrypt(p7, private_key_, certificate_, bio_out, 0)) {
    ERR_print_errors_fp(stderr);
    return false;
  }

  char* hex_key;

  //  Store the key
  buffer_size = BIO_get_mem_data(bio_out, &bio_buffer);
  hex_key = (char *)malloc(buffer_size);
  memcpy(hex_key, bio_buffer, buffer_size);
  BIO_free(bio_out);

  //UnHexlify(hex_key, buffer_size,(char**) &aes_key_); // TODO: CHECK IF IT IS NEEDED
  aes_key_ = (unsigned char*)malloc(buffer_size);
  memcpy(aes_key_, hex_key, buffer_size);

  return true;
}

/**
 * Loads Initialization Vector.
 * It should be 128 bits, taken from the last but one 32 hex characters
 * of the id of the encrypted file, that is to say [-33:-1].
 *
 * \param iv Hex string in ascii
 * \return True on success, false otherwise
 */
bool LoadIV(const char* iv, unsigned ivlen) {

 
  int nread;
  unsigned iv_strtol;

  //  Convert hex string to binary
  // UnHexlify((char*)iv, ivlen,(char**) &iv_);

  
  iv_ = (unsigned char*)malloc(ivlen);
  memcpy(iv_, iv, ivlen);

  return true;
 
}

int UnHexlify(char* str, unsigned len, char** buf) {

  int i;
  char* buff = (char*) malloc(sizeof(char)*len/2);
  char* pos = str;
  *buf = buff;

  for(i = 0; i < len/2; i++) {
    sscanf(pos, "%2hhx", &buff[i]);
    pos += 2;
  }

  return i;


} 


/**
 * Decrypt buffer using symmetric key.
 * Key and initialization vector (iv) must be loaded with
 * LoadAESKey() and LoadIV(), first; they are hexadecimal strings (in ASCII).
 * Buffer is base64 encoded.
 *
 * \return the length of decrypted data
 */
int Decrypt(const unsigned char *buffer,
	    const unsigned buffer_size,
	     const unsigned char **ptr)
{


  unsigned char* buf_out; //buffer for clear-text string
  int byte_out; //decoded bytes
  int byte_outF; //last decoded bytes

  BIO *b64, *bio; //  bio to base64decode the buffer
  int buff_size; //  total number of base64 decoded bytes
  void* buff_tmp; //  temp pointer to  decoded bytes
  char* buff; //  buffer to store decoded bytes

  // Prepare the context
  if(ctx_) free(ctx_);
  ctx_ = (EVP_CIPHER_CTX*)malloc(sizeof(EVP_CIPHER_CTX));
  EVP_CIPHER_CTX_init(ctx_);
  EVP_CIPHER_CTX_set_padding(ctx_, 1);

  // Base64 decode
  buff = (char*) buffer;
  buff_size = unbase64((const char*)buff, buffer_size,(const char**) &buff);
  //buff_size = strlen(buff);

  // Initialize decryption 
  if (!EVP_DecryptInit(ctx_, EVP_aes_256_cbc(), aes_key_, iv_))
    ERR_print_errors_fp(stderr);

  buf_out = (unsigned char*)malloc(sizeof(char)*(buff_size + EVP_CIPHER_CTX_block_size(ctx_))); //alloc enough space for padding
  memset(buf_out, 0, buff_size + EVP_CIPHER_CTX_block_size(ctx_));  //init to 0

  // Decrypt
  if (!EVP_DecryptUpdate(ctx_, buf_out, &byte_out,(const unsigned char*) buff, buff_size)){
    LogCvmfs(kLogCipher, kLogDebug, "Decryption error\n");
    ERR_print_errors_fp(stderr);
  }

  // Finalize
  if(!EVP_DecryptFinal_ex(ctx_, buf_out + byte_out, &byte_outF)){
    LogCvmfs(kLogCipher, kLogStderr, "Padding incorrect or wrong key/iv\n");
    ERR_print_errors_fp(stderr);
  }

  *ptr = buf_out;
  return(byte_out + byte_outF);

}

  int unbase64(const char *input, int length, const char** output){
  BIO *b64, *bmem;
  int nread;

  char *buffer = (char *)malloc(length);
  memset(buffer, 0, length);

  b64 = BIO_new(BIO_f_base64());
  BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
  bmem = BIO_new_mem_buf((void*)input, length);
  bmem = BIO_push(b64, bmem);

  nread = BIO_read(bmem, buffer, length);

  BIO_free_all(bmem);

  *output = buffer;
  return nread;
}


} //namespace cipher
