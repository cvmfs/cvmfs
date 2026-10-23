/**
 * This file is part of the CernVM File System.
 *
 * Configure-time probe for the deflate output of the zlib we link against.
 *
 * cvmfs content hashes are computed over the compressed stream, so a zlib that
 * produces different (but perfectly valid) deflate output silently changes
 * object names. This compresses a fixed pseudo-random buffer the way the
 * ingestion pipeline does and prints crc32(compressed):length, which the build
 * compares against the value produced by the vendored zlib.
 *
 * Kept C89-clean: it is compiled by try_run() with whatever flags the project
 * is configured with.
 */

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <zlib.h>

#define IN_BLOCK  (4096 * 4) /* TaskRead::kBlockSize */
#define OUT_BLOCK (4096 * 2) /* TaskCompress::kCompressedBlockSize */
#define TOTAL     (64 * 1024)

int main(void) {
  static unsigned char data[TOTAL];
  unsigned char outbuf[OUT_BLOCK];
  z_stream strm;
  uint64_t state = 1337; /* cvmfs Prng, MMIX constants */
  uLong crc;
  size_t i, pos = 0, total_out = 0;
  int done = 0;

  for (i = 0; i < TOTAL; ++i) {
    double scaled;
    state = 6364136223846793005ULL * state + 1442695040888963407ULL;
    scaled = (double)state * 256.0 / 18446744073709551616.0;
    data[i] = (unsigned char)((uint64_t)scaled % 256);
  }

  memset(&strm, 0, sizeof(strm));
  if (deflateInit(&strm, Z_DEFAULT_COMPRESSION) != Z_OK)
    return 1;
  crc = crc32(0L, Z_NULL, 0);

  while (!done) {
    unsigned char *in_ptr;
    size_t in_size, remaining_in;
    int flush, inner_done = 0;

    in_size = TOTAL - pos;
    if (in_size > IN_BLOCK)
      in_size = IN_BLOCK;
    flush = (pos + in_size >= TOTAL);
    in_ptr = data + pos;
    remaining_in = in_size;
    pos += in_size;

    /* mirrors TaskCompress::Process() */
    do {
      int rc;
      size_t produced;

      strm.next_in = in_ptr;
      strm.avail_in = (uInt)remaining_in;
      strm.next_out = outbuf;
      strm.avail_out = OUT_BLOCK;
      rc = deflate(&strm, flush ? Z_FINISH : Z_NO_FLUSH);
      if (rc != Z_OK && rc != Z_STREAM_END)
        return 1;
      produced = OUT_BLOCK - strm.avail_out;
      crc = crc32(crc, outbuf, (uInt)produced);
      total_out += produced;
      in_ptr = strm.next_in;
      remaining_in = strm.avail_in;
      inner_done = flush ? (rc == Z_STREAM_END)
                         : (rc == Z_OK && strm.avail_in == 0);
    } while (remaining_in > 0 || (flush && !inner_done));

    if (flush)
      done = 1;
  }
  deflateEnd(&strm);

  printf("%08lx:%lu\n", (unsigned long)crc, (unsigned long)total_out);
  return 0;
}
