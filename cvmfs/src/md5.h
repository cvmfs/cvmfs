#ifndef MD5_H
#define MD5_H 1

#include <openssl/md5.h>

typedef MD5_CTX md5_state_t;

static inline void md5_init(md5_state_t *pms) 
{
   MD5_Init(pms);
}

static inline void md5_append(md5_state_t *pms, const unsigned char *buf, int len) 
{
   MD5_Update(pms, buf, len);
}

static inline void md5_finish(md5_state_t *pms, unsigned char digest[16]) 
{
   MD5_Final(digest, pms);
}

#endif
