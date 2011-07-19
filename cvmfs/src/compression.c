/**
 * \file compression.c
 *
 * This is a wrapper around zlib.  It provides
 * a set of functions to conveniently compress and decompress stuff.
 * Allmost all of the functions return zero on success, otherwise non-zero.
 *
 * Developed by Jakob Blomer 2009 at CERN
 * jakob.blomer@cern.ch
 */

#define _FILE_OFFSET_BITS 64

#include "compression.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include "zlib-duplex.h"
#include "debug.h"
#include "sha1.h"
#include "smalloc.h"

#define BUFSIZE 32768

int compress_strm_init(z_stream *strm)
{
   strm->zalloc = Z_NULL;
   strm->zfree = Z_NULL;
   strm->opaque = Z_NULL;
   strm->next_in = Z_NULL;
   strm->avail_in = 0;
   return deflateInit(strm, Z_DEFAULT_COMPRESSION);
}


int compress_estrm_init(struct z_estream *estrm)
{
   int result = compress_strm_init(&estrm->strm);
   estrm->strm.next_out = estrm->out;
   estrm->strm.avail_out = Z_CHUNK;
   estrm->flush = Z_NO_FLUSH;
   estrm->rgauge = 0;
   estrm->have = 0;
   estrm->eof = 0;
   return result;
}


int decompress_strm_init(z_stream *strm)
{
   strm->zalloc = Z_NULL;
   strm->zfree = Z_NULL;
   strm->opaque = Z_NULL;
   strm->avail_in = 0;
   strm->next_in = Z_NULL;
   return inflateInit(strm);
}


void compress_strm_fini(z_stream *strm)
{
   (void)deflateEnd(strm);
}


void compress_estrm_fini(struct z_estream *estrm)
{
   compress_strm_fini(&estrm->strm);
}


void decompress_strm_fini(z_stream *strm)
{
   (void)inflateEnd(strm);
}


/**
 * \return -1 (error), 0 (sucessful decompressed chunk), 1 (success, end of stream)
 */
int decompress_strm_file(z_stream *strm, FILE *f, const void *buf, const size_t size)
{
   unsigned char out[Z_CHUNK];
   int z_ret;
   size_t pos = 0;
   
   do {
      strm->avail_in = (Z_CHUNK > (size-pos)) ? size-pos : Z_CHUNK;
      strm->next_in = ((unsigned char *)buf)+pos;

      /* run inflate() on input until output buffer not full */
      do {
         strm->avail_out = Z_CHUNK;
         strm->next_out = out;
         z_ret = inflate(strm, Z_NO_FLUSH);
         switch (z_ret) {
            case Z_NEED_DICT:
               z_ret = Z_DATA_ERROR;     /* and fall through */
            case Z_STREAM_ERROR:
            case Z_DATA_ERROR:
            case Z_MEM_ERROR:
               return -1;
         }
         size_t have = Z_CHUNK - strm->avail_out;
         if (fwrite(out, 1, have, f) != have || ferror(f))
            return -1;
      } while (strm->avail_out == 0);

      pos += Z_CHUNK;
   } while (pos < size);
   
   return (z_ret == Z_STREAM_END ? 1 : 0);  
}


/**
 * User of this function has to free out_buf. 
 */
int decompress_mem(const void *buf, const size_t size, void **out_buf, size_t *out_size)
{
   unsigned char out[Z_CHUNK];
   int z_ret;
   z_stream strm;
   size_t pos = 0;
   size_t alloc_size = Z_CHUNK;
      
   if (decompress_strm_init(&strm) != Z_OK) {
      *out_buf = NULL;
      *out_size = 0;
      return -1;
   }
   
   *out_buf = smalloc(alloc_size);
   *out_size = 0;
   
   do {
      strm.avail_in = (Z_CHUNK > (size-pos)) ? size-pos : Z_CHUNK;
      strm.next_in = ((unsigned char *)buf)+pos;

      /* run inflate() on input until output buffer not full */
      do {
         strm.avail_out = Z_CHUNK;
         strm.next_out = out;
         z_ret = inflate(&strm, Z_NO_FLUSH);
         switch (z_ret) {
            case Z_NEED_DICT:
               z_ret = Z_DATA_ERROR;     /* and fall through */
            case Z_STREAM_ERROR:
            case Z_DATA_ERROR:
            case Z_MEM_ERROR:
               decompress_strm_fini(&strm);
               free(*out_buf);
               *out_buf = NULL;
               *out_size = 0;
               return -1;
         }
         size_t have = Z_CHUNK - strm.avail_out;
         if (*out_size+have > alloc_size)
         {
            alloc_size *= 2;
            *out_buf = srealloc(*out_buf, alloc_size);
         }
         memcpy(((unsigned char *)*out_buf)+*out_size, out, have);
         *out_size += have;
      } while (strm.avail_out == 0);

      pos += Z_CHUNK;
   } while (pos < size);
   
   decompress_strm_fini(&strm);
   if (z_ret != Z_STREAM_END)
   {
      free(*out_buf);
      *out_buf = NULL;
      *out_size = 0;
      return -1;
   } else return 0;
}


/**
 * User of this function has to free out_buf. 
 */
int compress_mem(const void *buf, const size_t size, void **out_buf, size_t *out_size)
{
   unsigned char out[Z_CHUNK];
   int z_ret;
   int flush;
   z_stream strm;
   size_t pos = 0;
   size_t alloc_size = Z_CHUNK;

   if (compress_strm_init(&strm) != Z_OK) return -1;
      
   *out_buf = smalloc(alloc_size);
   *out_size = 0;
   
   do {
      strm.avail_in = (Z_CHUNK > (size-pos)) ? size-pos : Z_CHUNK;
      flush = (pos + Z_CHUNK) >= size ? Z_FINISH : Z_NO_FLUSH;
      strm.next_in = ((unsigned char *)buf)+pos;

      /* run deflate() on input until output buffer not full */
      do {
         strm.avail_out = Z_CHUNK;
         strm.next_out = out;
         z_ret = deflate(&strm, flush);
         if (z_ret == Z_STREAM_ERROR) {
               compress_strm_fini(&strm);
               free(*out_buf);
               return -1;
         }
         size_t have = Z_CHUNK - strm.avail_out;
         if (*out_size+have > alloc_size)
         {
            alloc_size *= 2;
            *out_buf = srealloc(*out_buf, alloc_size);
         }
         memcpy(((unsigned char *)*out_buf)+*out_size, out, have);
         *out_size += have;
      } while (strm.avail_out == 0);

      pos += Z_CHUNK;
   } while (flush != Z_FINISH);
   
   compress_strm_fini(&strm);
   if (z_ret != Z_STREAM_END)
   {
      free(*out_buf);
      return -1;
   } else return 0;
}


int compress_file_fp(FILE *fsrc, FILE *fdest)
{
   int z_ret, flush, result = -1;
   unsigned have;
   z_stream strm;
   unsigned char in[Z_CHUNK];
   unsigned char out[Z_CHUNK];
   
   if (compress_strm_init(&strm) != Z_OK) goto compress_file_fp_final;

   /* compress until end of file */
   do {
      strm.avail_in = fread(in, 1, Z_CHUNK, fsrc);
      if (ferror(fsrc)) goto compress_file_fp_final; 

      flush = feof(fsrc) ? Z_FINISH : Z_NO_FLUSH;
      strm.next_in = in;

      /* run deflate() on input until output buffer not full, finish
         compression if all of source has been read in */
      do {
         strm.avail_out = Z_CHUNK;
         strm.next_out = out;
         z_ret = deflate(&strm, flush);    /* no bad return value */
         if (z_ret == Z_STREAM_ERROR) goto compress_file_fp_final;  /* state not clobbered */
         have = Z_CHUNK - strm.avail_out;
         if (fwrite(out, 1, have, fdest) != have || ferror(fdest))
            goto compress_file_fp_final;
      } while (strm.avail_out == 0);
      
   /* done when last data in file processed */
   } while (flush != Z_FINISH);
 
   /* stream will be complete */
   if (z_ret != Z_STREAM_END) goto compress_file_fp_final;
   
   result = 0;        

   /* clean up and return */
compress_file_fp_final:
   compress_strm_fini(&strm);
   pmesg(D_COMPRESS, "file compression finished with error code %d", result);
   return result;
}


int compress_file_fp_sha1(FILE *fsrc, FILE *fdest, unsigned char digest[20])
{
   int z_ret, flush, result = -1;
   unsigned have;
   z_stream strm;
   unsigned char in[Z_CHUNK];
   unsigned char out[Z_CHUNK];
   sha1_context_t sha1_ctx;
   
   if (compress_strm_init(&strm) != Z_OK) goto compress_file_fp_sha1_final;
   sha1_init(&sha1_ctx);

   /* compress until end of file */
   do {
      strm.avail_in = fread(in, 1, Z_CHUNK, fsrc);
      if (ferror(fsrc)) goto compress_file_fp_sha1_final; 

      flush = feof(fsrc) ? Z_FINISH : Z_NO_FLUSH;
      strm.next_in = in;

      /* run deflate() on input until output buffer not full, finish
         compression if all of source has been read in */
      do {
         strm.avail_out = Z_CHUNK;
         strm.next_out = out;
         z_ret = deflate(&strm, flush);    /* no bad return value */
         if (z_ret == Z_STREAM_ERROR) goto compress_file_fp_sha1_final;  /* state not clobbered */
         have = Z_CHUNK - strm.avail_out;
         if (fwrite(out, 1, have, fdest) != have || ferror(fdest))
            goto compress_file_fp_sha1_final;
         sha1_update(&sha1_ctx, out, have);
      } while (strm.avail_out == 0);
      
   /* done when last data in file processed */
   } while (flush != Z_FINISH);
 
   /* stream will be complete */
   if (z_ret != Z_STREAM_END) goto compress_file_fp_sha1_final;
   
   sha1_final(digest, &sha1_ctx);
   result = 0;        

   /* clean up and return */
compress_file_fp_sha1_final:
   compress_strm_fini(&strm);
   pmesg(D_COMPRESS, "file compression finished with error code %d", result);
   return result;
}


int compress_file_sha1_only(FILE *fsrc, unsigned char digest[20])
{
   int z_ret, flush, result = -1;
   unsigned have;
   z_stream strm;
   unsigned char in[Z_CHUNK];
   unsigned char out[Z_CHUNK];
   sha1_context_t sha1_ctx;
   
   if (compress_strm_init(&strm) != Z_OK) goto compress_file_sha1_only_final;
   sha1_init(&sha1_ctx);

   /* compress until end of file */
   do {
      strm.avail_in = fread(in, 1, Z_CHUNK, fsrc);
      if (ferror(fsrc)) goto compress_file_sha1_only_final; 

      flush = feof(fsrc) ? Z_FINISH : Z_NO_FLUSH;
      strm.next_in = in;

      /* run deflate() on input until output buffer not full, finish
         compression if all of source has been read in */
      do {
         strm.avail_out = Z_CHUNK;
         strm.next_out = out;
         z_ret = deflate(&strm, flush);    /* no bad return value */
         if (z_ret == Z_STREAM_ERROR) goto compress_file_sha1_only_final;  /* state not clobbered */
         have = Z_CHUNK - strm.avail_out;
         sha1_update(&sha1_ctx, out, have);
      } while (strm.avail_out == 0);
      
   /* done when last data in file processed */
   } while (flush != Z_FINISH);
 
   /* stream will be complete */
   if (z_ret != Z_STREAM_END) goto compress_file_sha1_only_final;
   
   sha1_final(digest, &sha1_ctx);
   result = 0;        

   /* clean up and return */
compress_file_sha1_only_final:
   compress_strm_fini(&strm);
   pmesg(D_COMPRESS, "file compression finished with error code %d", result);
   return result;
}



int read_and_compress(FILE *fsrc, struct z_estream *estrm, void *buf, const int buf_size)
{
   if (buf_size <= 0) return buf_size;
   if (estrm->eof) return 0;
   
   int buf_pos = 0;
   
read_and_compress_next_chunk:
   /* fill from the estrm out-buffer as much as possible */
   if (estrm->rgauge < estrm->have)
   {
      unsigned nbytes = ((unsigned)(buf_size - buf_pos) > (estrm->have - estrm->rgauge)) ? 
         estrm->have - estrm->rgauge : (unsigned)(buf_size - buf_pos);
      memcpy(buf+buf_pos, estrm->out + estrm->rgauge, nbytes);
      estrm->rgauge += nbytes;
      buf_pos += nbytes;
      if (buf_pos == buf_size) return buf_size;
   }
   estrm->rgauge = 0;
   
read_and_compress_deflate:
   /* run deflate() on input until output buffer not full */
   if (estrm->strm.avail_out == 0)
   {
      estrm->strm.avail_out = Z_CHUNK;
      estrm->strm.next_out = estrm->out;
      estrm->z_ret = deflate(&(estrm->strm), estrm->flush);
      if (estrm->z_ret == Z_STREAM_ERROR) return -1;
      estrm->have = Z_CHUNK - estrm->strm.avail_out;
      goto read_and_compress_next_chunk; 
   }

   /* read next chunk of file */
   if (estrm->flush != Z_FINISH) {
      estrm->strm.avail_in = fread(estrm->in, 1, Z_CHUNK, fsrc);
      //pmesg(D_COMPRESS, "read %d bytes", estrm->strm.avail_in);
      if (ferror(fsrc)) return -1; 

      estrm->flush = feof(fsrc) ? Z_FINISH : Z_NO_FLUSH;
      estrm->strm.next_in = estrm->in;
      
      /* Marker to jump into deflate */
      estrm->strm.avail_out = 0;

      goto read_and_compress_deflate;
   };
 
   if (estrm->z_ret != Z_STREAM_END) return -1;

   estrm->eof = 1;
   return buf_pos;
}


int compress_file(const char *src, const char *dest)
{   
   FILE *fsrc = fopen(src, "r");
   if (!fsrc) {
      pmesg(D_COMPRESS, "open %s as compression source failed", src);
      return -1;
   }
   
   FILE *fdest = fopen(dest, "w");
   if (!fdest) 
   {
      pmesg(D_COMPRESS, "open %s as compression destination failed", dest);
      fclose(fsrc);
      return -1;
   }
   
   pmesg(D_COMPRESS, "opened %s and %s for compression", src, dest);
   const int result = compress_file_fp(fsrc, fdest);

   fclose(fsrc);
   fclose(fdest);
   return result;
}


int compress_file_sha1(const char *src, const char *dest, unsigned char digest[20])
{   
   FILE *fsrc = fopen(src, "r");
   if (!fsrc) {
      pmesg(D_COMPRESS, "open %s as compression source failed", src);
      return -1;
   }
   
   FILE *fdest = fopen(dest, "w");
   if (!fdest) 
   {
      pmesg(D_COMPRESS, "open %s as compression destination failed", dest);
      fclose(fsrc);
      return -1;
   }
   
   pmesg(D_COMPRESS, "opened %s and %s for compression", src, dest);
   struct stat info;
   int result = compress_file_fp_sha1(fsrc, fdest, digest) ||
                fstat(fileno(fsrc), &info) ||
                fchmod(fileno(fdest), info.st_mode);

   fclose(fsrc);
   fclose(fdest);
   return result;
}


static int file_copy_fp(FILE *fsrc, FILE *fdest) 
{
   unsigned char buf[1024];
   rewind(fsrc);
   rewind(fdest);
   
   size_t have;
   do {
      have = fread(buf, 1, 1024, fsrc);
      if (fwrite(buf, 1, have, fdest) != have)
         return -1;
   } while (have == 1024);
   return 0;
}


int file_copy(const char *src, const char *dest)
{
   FILE *fsrc = NULL;
   FILE *fdest = NULL;
   int result = -1;
   struct stat info;
      
   fsrc = fopen(src, "r");
   if (!fsrc) goto file_copy_final;
   
   fdest = fopen(dest, "w");
   if (!fdest) goto file_copy_final;
   
   result = file_copy_fp(fsrc, fdest);
   result |= fstat(fileno(fsrc), &info);
   result |= fchmod(fileno(fdest), info.st_mode);
   
file_copy_final:
   if (fsrc) fclose(fsrc);
   if (fdest) fclose(fdest);
   return result;
}


int decompress_file_fp(FILE *fsrc, FILE *fdest) {
   int result = -1;
   int eos = -1;
   z_stream strm;
   size_t have;
   unsigned char buf[BUFSIZE];
   
   if (decompress_strm_init(&strm) != Z_OK) goto decompress_file_fp_final;
   
   while ((have = fread(buf, 1, BUFSIZE, fsrc)) > 0)
   {
      if ((eos = decompress_strm_file(&strm, fdest, buf, have)) < 0)
         goto decompress_file_fp_final; 
   }
   pmesg(D_COMPRESS, "End of decompression, eos=%d, error=%d", eos, ferror(fsrc));
   if ((eos != 1) || ferror(fsrc)) goto decompress_file_fp_final;
   
   result = 0;

decompress_file_fp_final:
   decompress_strm_fini(&strm);
   return result;
}


int decompress_file(const char *src, const char *dest)
{
   FILE *fsrc = NULL;
   FILE *fdest = NULL;
   int result = -1;
      
   fsrc = fopen(src, "r");
   if (!fsrc) goto decompress_file_final;
   
   fdest = fopen(dest, "w");
   if (!fdest) goto decompress_file_final;
   
   result = decompress_file_fp(fsrc, fdest);
   
decompress_file_final:
   if (fsrc) fclose(fsrc);
   if (fdest) fclose(fdest);
   return result;
}

