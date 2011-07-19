#ifndef COMPRESSION_H
#define COMPRESSION_H 1

#include <stdio.h>
#include "zlib-duplex.h"
#include "sha1.h"

#define Z_CHUNK 16384

struct z_estream {
   z_stream strm;
   int z_ret;
   int flush;
   int eof;
   unsigned have;
   unsigned rgauge;
   unsigned char in[Z_CHUNK];
   unsigned char out[Z_CHUNK];
};

int compress_strm_init(z_stream *strm);
int compress_estrm_init(struct z_estream *estrm);
int decompress_strm_init(z_stream *strm);
void compress_strm_fini(z_stream *strm);
void compress_estrm_fini(struct z_estream *estrm);
void decompress_strm_fini(z_stream *strm);

int read_and_compress(FILE *fsrc, struct z_estream *estrm, void *buf, const int buf_size);

/* returns -1 (error), 0 (sucessful decompressed chunk), 1 (success, end of stream) */
int decompress_strm_file(z_stream *strm, FILE *f, const void *buf, const size_t size);


/* User of these functions has to free out_buf */
int decompress_mem(const void *buf, const size_t size, void **out_buf, size_t *out_size);
int compress_mem(const void *buf, const size_t size, void **out_buf, size_t *out_size);

int compress_file(const char *src, const char *dest);
int compress_file_sha1(const char *src, const char *dest, unsigned char digest[20]);
int compress_file_sha1_only(FILE *fsrc, unsigned char digest[20]);
int compress_file_fp(FILE *fsrc, FILE *fdest);
int compress_file_fp_sha1(FILE *fsrc, FILE *fdest, unsigned char digest[20]);
//int compress_file_inplace(const char *name);
int decompress_file(const char *src, const char *dest);
int decompress_file_fp(FILE *fsrc, FILE *fdest);

int file_copy(const char *src, const char *dest);

#endif

