/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_COMPRESSION_H_
#define CVMFS_COMPRESSION_COMPRESSION_H_

#include <errno.h>
#include <stdint.h>
#include <stdio.h>

#include <string>

#include "duplex_zlib.h"
#include "input_abstract.h"
#include "network/sink.h"
#include "network/sink_mem.h"
#include "util.h"
#include "util/plugin.h"

namespace shash {
struct Any;
class ContextPtr;
}

namespace zlib {

/**
 * Abstract Compression class which is inherited by implementations of
 * compression engines such as zlib.
 *
 * In order to add a new compression method, you simply need to add a new class
 * which is a sub-class of the Compressor. The subclass needs to implement the
 * Deflate, DeflateBound, Clone, and WillHandle functions.  For information on
 * the WillHandle function, read up on the PolymorphicConstruction class.
 * The new sub-class must be listed in the implementation of the
 * Compressor::RegisterPlugins function.
 *
 */
class Compressor: public PolymorphicConstruction<Compressor, Algorithms> {
 public:
  explicit Compressor(const Algorithms & /* alg */) : kZChunk(16384) { }
  virtual ~Compressor() { }
  /**
   * Compression function.
   * Takes a read-only data source, compresses the data and writes the result to
   * a given sink. 
   * 
   * Must be able to handle empty sources and just write the compression frame
   * where applicable.
   * 
   * @return kStreamEnd if successful
   *         StreamState Error value if failure
   */
  virtual StreamStates Compress(InputAbstract * input, cvmfs::Sink *output) = 0;
  /**
   *  Same like Compress() but also calculates the hash based on the
   *  compressed output.
   */
  virtual StreamStates Compress(InputAbstract *input, cvmfs::Sink *output,
                                shash::Any *compressed_hash) = 0;

    /**
   * Deflate function.  The arguments and returns closely match the input and
   * output of the zlib deflate function.
   * Input:
   *   - outbuf - Output buffer to write the compressed data.
   *   - outbufsize - Size of the output buffer
   *   - inbuf - Input data to be compressed
   *   - inbufsize - Size of the input buffer
   *   - flush - Whether the compression stream should be flushed / finished
   * Upon return:
   *   returns: true - if done compressing, false otherwise
   *   - outbuf - output buffer pointer (unchanged from input)
   *   - outbufsize - The number of bytes used in the outbuf
   *   - inbuf - Pointer to the next byte of input to read in
   *   - inbufsize - the remaining bytes of input to read in.
   *   - flush - unchanged from input
   */
  // TODO(heretherebedragons) remove! when everything is replaced to use the
  // compressor
  virtual StreamStates CompressStream(InputAbstract *input,
                                  cvmfs::MemSink *output, const bool flush) = 0;
  /**
   * Reset stream to perform compression on a new, independent input
   */
  virtual bool Reset() = 0;
  virtual size_t CompressUpperBound(const size_t bytes) = 0;

  virtual Compressor* Clone() = 0;
  virtual std::string Describe() = 0;

  static void RegisterPlugins();

 protected:
  const unsigned kZChunk;
};

}  // namespace zlib

#endif  // CVMFS_COMPRESSION_COMPRESSION_H_
