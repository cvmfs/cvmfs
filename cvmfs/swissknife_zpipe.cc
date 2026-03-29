/**
 * This file is part of the CernVM File System.
 *
 * Slightly adapted zpipe.c for the use within the CernVM File System
 */

/* zpipe.c: example of proper use of zlib's inflate() and deflate()
   Not copyrighted -- provided to the public domain
   Version 1.4  11 December 2005  Mark Adler */

/* Version history:
   1.0  30 Oct 2004  First version
   1.1   8 Nov 2004  Add void casting for unused return values
                     Use switch statement for inflate() return values
   1.2   9 Nov 2004  Add assertions to document zlib guarantees
   1.3   6 Apr 2005  Remove incorrect assertion in inf()
   1.4  11 Dec 2005  Add hack to avoid MSDOS end-of-line conversions
                     Avoid some compiler warnings for input and output buffers
 */

#include <stdio.h>

#include <cassert>
#include <cstring>

#include "swissknife_zpipe.h"
#include "compression/compressor.h"
#include "compression/decompressor.h"
#include "compression/input_file.h"
#include "network/sink_file.h"

#if defined(MSDOS) || defined(OS2) || defined(WIN32) || defined(__CYGWIN__)
#  include <fcntl.h>
#  include <io.h>
#  define SET_BINARY_MODE(file) setmode(fileno(file), O_BINARY)
#else
#  define SET_BINARY_MODE(file)
#endif

#define CHUNK 16384

/* compress or decompress from stdin to stdout */
int swissknife::CommandZpipe::Main(const swissknife::ArgumentList &args) {
    /* avoid end-of-line conversions */
    SET_BINARY_MODE(stdin);
    SET_BINARY_MODE(stdout);

    auto input = new zip::InputFile(stdin, CHUNK, /*is_owner=*/true);
    auto output = new cvmfs::FileSink(stdout);

    /* do compression if no arguments */
    if (args.find('d') == args.end()) {
        zip::Algorithm comp_alg = zip::CompressionAlgFromEnv();
        auto *compressor = zip::Compressor::Construct(comp_alg);
        const zip::StreamStates res = compressor->Compress(input, output);
        if (res == zip::kStreamEnd) {
            return 0;
        } else {
            fprintf(stderr, "Compression returned %d", res);
            return 1;
        }
    } else {
        /* do decompression if -d specified */
        zip::Algorithm decomp_alg = zip::DecompressionAlgFromEnv();
        auto *decompressor = zip::Decompressor::Construct(decomp_alg);
        const zip::StreamStates res = decompressor->DecompressStream(input, output);
        if (res == zip::kStreamEnd) {
            return 0;
        } else {
            fprintf(stderr, "Decompression returned %d", res);
            return 1;
        }
    }
}
