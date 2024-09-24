/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_DECOMPRESSION_H_
#define CVMFS_COMPRESSION_DECOMPRESSION_H_


#include <errno.h>
#include <stdint.h>
#include <stdio.h>

#include <string>

#include "duplex_zlib.h"  // TODO(heretherebedragons) remove later
#include "input_abstract.h"
#include "network/sink.h"
#include "util.h"
#include "util/plugin.h"

namespace shash {
struct Any;
class ContextPtr;
}

namespace zlib {

/**
 * Abstract Decompression class which is inherited by implementations of
 * compression engines such as zlib.
 *
 * In order to add a new decompression method, you simply need to add a new
 * class which is a sub-class of the Decompressor. The subclass must implement
 * the DecompressStream, Clone, and WillHandle functions.
 *
 * @note For information on the WillHandle function, read up on the
 *       PolymorphicConstruction class.
 * @note The new sub-class must be listed in the implementation of the
 *       Compressor::RegisterPlugins function.
 *
 */
class Decompressor : public PolymorphicConstruction<Decompressor, Algorithms> {
 public:
  explicit Decompressor(const Algorithms & /*alg*/) : kZChunk_(16384) { }
  Decompressor(const Algorithms & /*alg*/, size_t zchunk) : kZChunk_(zchunk) { }
  virtual ~Decompressor() { }
  /**
   * Compression function.
   * Takes a read-only data source, compresses the data and writes the result to
   * a given sink.
   *
   * Must be able to handle empty sources and just write the compression frame
   * where applicable.
   *
   * @return kStreamEnd if successful and compression stream finished
   *         kStreamContinue if successful and compression stream is unfinished
   *         StreamState Error value if failure
   */
  // TODO(heretherebedragons) maybe rename it just to Decompress()?
  virtual StreamStates DecompressStream(InputAbstract *input,
                                        cvmfs::Sink *output) = 0;
  /**
   * Reset stream to perform decompression on a new, independent input
   */
  virtual bool Reset() = 0;
  virtual size_t kZChunk() const { return kZChunk_; }

  virtual Decompressor* Clone() = 0;
  virtual std::string Describe() = 0;

  static void RegisterPlugins();

 protected:
  const size_t kZChunk_;
};

}  // namespace zlib

#endif  // CVMFS_COMPRESSION_DECOMPRESSION_H_
