#ifndef SHA1_H
#define SHA1_H 1

#include <openssl/sha.h>
#include <stdio.h>

#define SHA1_DIGEST_LENGTH 20
#define SHA1_DIGEST_ASCII_LENGTH 42

typedef SHA_CTX sha1_context_t;

static inline void sha1_init(sha1_context_t *ctx) 
{
   SHA1_Init(ctx);
}

static inline void sha1_update(sha1_context_t *ctx, const unsigned char *buf, unsigned len)
{
   SHA1_Update(ctx, buf, len);
}


static inline void sha1_final(unsigned char digest[SHA1_DIGEST_LENGTH], sha1_context_t *ctx)
{
   SHA1_Final(digest, ctx);
}

void sha1_mem(const void *buf, const unsigned buf_size, unsigned char digest[SHA1_DIGEST_LENGTH]);
void sha1_file_fp(FILE *fp, unsigned char digest[SHA1_DIGEST_LENGTH]);
int sha1_file(const char *filename, unsigned char digest[SHA1_DIGEST_LENGTH]);
void sha1_string( const unsigned char digest[SHA1_DIGEST_LENGTH], char sha1_str[41] );

#endif
