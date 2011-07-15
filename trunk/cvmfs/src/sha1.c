/**
 * \file sha1.c
 */

#define _FILE_OFFSET_BITS 64

#include "sha1.h"
#include "debug.h"
#include <stdio.h>

#define BUFFER_SIZE 4096

void sha1_file_fp(FILE *fp, unsigned char digest[SHA1_DIGEST_LENGTH])
{
   sha1_context_t context;
   int actual;
	unsigned char buffer[BUFFER_SIZE];
   
   sha1_init(&context);
	while ((actual = fread(buffer, 1, BUFFER_SIZE, fp))) {
		sha1_update(&context, buffer, actual);
	}
	sha1_final(digest, &context);
}


int sha1_file(const char *filename, unsigned char digest[SHA1_DIGEST_LENGTH])
{
	FILE *file;
	
   file = fopen(filename, "rb");
	if(!file) return 1;

   pmesg(D_HASH, "checksumming file %s", filename);
	sha1_file_fp(file, digest);

	fclose(file);
	return 0;
}


void sha1_string(const unsigned char digest[20], char sha1_str[41])
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
              unsigned char digest[SHA1_DIGEST_LENGTH]) 
{
   sha1_context_t ctx;
   sha1_init(&ctx);
   sha1_update(&ctx, (const unsigned char *)buf, buf_size);
   sha1_final(digest, &ctx);
}
