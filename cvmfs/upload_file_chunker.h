/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_FILE_CHUNKER_H_
#define CVMFS_UPLOAD_FILE_CHUNKER_H_

#include <utility>
#include <vector>
#include <cassert>

#include "util.h"

namespace upload {

/**
 * Defines a simple file chunk by providing an offset and the chunk size
 */
class Chunk : private std::pair<off_t, size_t> {
 public:
  Chunk(const off_t offset, const size_t size) :
    std::pair<off_t, size_t>(offset, size) {}

  inline off_t offset() const { return this->first; }
  inline size_t size() const   { return this->second; }
};

/**
 * Abstract base class of a chunk generator. Uses the PolymorphicConstruction
 * template to allow for easy addition of new concrete ChunkGenerators.
 *
 * Derived classes mainly need to override the `FindNextCutMark()` method that
 * returns the offset in the file where a new chunk should start. This method
 * gets called from the `Next()` which provides the next Chunk to be processed.
 *
 * Also it provides the global configuration for the roughly desired chunk sizes
 * which should be obeyed by each concrete implementation of ChunkGenerator. It
 * lies in the responsiblity of the concrete implementations to actually make
 * sure that the chunks stay inside this limitiations!
 *
 * The idea to have more than one concrete implementation of ChunkGenerator is
 * to cut different file types differently. For example one could cut text files
 * at line breaks, while binary files could be cut at 0x0 bytes.
 *
 * Example Usage:
 *
 *   ChunkGenerator *generator = ChunkGenerator::Construct(mmf);
 *   while(generator->HasMoreData()) {
 *     Chunk chunk = generator->Next();
 *     process(chunk);
 *   }
 *   delete generator;
 *
 */
class ChunkGenerator : public PolymorphicConstruction<ChunkGenerator,
                                                      MemoryMappedFile>,
                       private SingleCopy {
 public:
  ChunkGenerator(const MemoryMappedFile &mmf);
  virtual ~ChunkGenerator() {};

  /**
   * Find the next Chunk of the file to be processed.
   * @return  the next Chunk to be processed
   */
  Chunk Next();

  /**
   * Checks whether the whole file was processed. If true, you must not call
   * `Next()` on this ChunkGenerator anymore!
   * @return  true if there is at least one more chunk to come
   */
  bool HasMoreData() const;

  static void RegisterPlugins();

 protected:
  inline const MemoryMappedFile& mmf()    const { return mmf_; }
  inline off_t                   offset() const { return offset_; }

  /**
   * Abstract method that needs to be overridden by derived ChunkGenerators.
   * In derived classes this should figure out at which position the file should
   * be cut successively.
   *
   * @return  the offset in the file where the next chunk should be chopped off
   */
  virtual off_t FindNextCutMark() const = 0;

  friend class AbstractSpooler;
  /**
   * Configures the chunk size restrictions each ChunkGenerator should obey.
   * This method is called by AbstractSpooler once, before any files get pro-
   * cessed.
   */
  static void SetFileChunkRestrictions(const size_t minimal_chunk_size,
                                       const size_t average_chunk_size,
                                       const size_t maximal_chunk_size);

 protected:
  static size_t minimal_chunk_size_;
  static size_t average_chunk_size_;
  static size_t maximal_chunk_size_;

 private:

  const MemoryMappedFile           &mmf_;
  off_t                             offset_;
};


/**
 * This provides the most simple implementation of a ChunkGenerator. It cuts
 * files blindly at a fixed chunk size.
 *
 * This should be seen as a last resort and will automatically be used, if no
 * other ChunkGenerator decides to take responsibility for a given file type.
 */
class NaiveChunkGenerator : public ChunkGenerator {
 public:
  NaiveChunkGenerator(const MemoryMappedFile &mmf) : ChunkGenerator(mmf) {};
  static bool  WillHandle(const MemoryMappedFile &mmf);

 protected:
  off_t        FindNextCutMark() const;
};

}

#endif /* CVMFS_UPLOAD_FILE_CHUNKER_H_ */
