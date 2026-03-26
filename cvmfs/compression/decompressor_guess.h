/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_DECOMPRESSOR_GUESS_H_
#define CVMFS_COMPRESSION_DECOMPRESSOR_GUESS_H_

#include <string>

#include "decompressor.h"

namespace zip {

/**
 * GuessDecompressor is a decompressor that tries to guess and use the right actual decompressor,
 * by analysing the first bytes and/or by trying to decompress successfully.
 */
class GuessDecompressor: public Decompressor {
 public:
  explicit GuessDecompressor(const Algorithms &alg);

    /**
   * Compression function.
   * Takes a read-only data source, compresses the data and writes the result to
   * a given sink.
   *
   * Must be able to handle empty sources and just write the compression frame
   * where applicable.
   *
   * @return kStreamEnd if successful and compression stream finished
   *         kStreamContinue <not applicable for GuessDecompressor>
   *         StreamState Error value if failure
   */
  virtual StreamStates DecompressStream(InputAbstract *input,
                                      cvmfs::Sink *output);
  /**
   * Reset stream to perform decompression on a new, independent input
   */
  virtual bool Reset() {
    delete backend_;
    backend_ = NULL;
    return true;
  }
  virtual Decompressor* Clone();
  virtual std::string Describe();
  static bool WillHandle(const zip::Algorithms &alg);

 private:
  Decompressor *backend_;
  zip::Algorithm alg_;

  void Guess(InputAbstract* input, cvmfs::Sink* output);
};

}  // namespace zip

#endif  // CVMFS_COMPRESSION_DECOMPRESSOR_GUESS_H_
